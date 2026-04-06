"""
Performance review for the wallet intelligence system.

Evaluates signal and category performance, flags underperformers,
and recommends changes. Advisory only — never auto-promotes or
auto-demotes.

Usage::

    from trading_platform.polymarket.performance_monitor import run_performance_review
    result = run_performance_review()
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from trading_platform.polymarket.wallet_db import WalletDB


def run_performance_review(db: WalletDB | None = None) -> dict[str, Any]:
    """Run comprehensive performance review. Returns advisory report."""
    db = db or WalletDB()

    # 1. Category performance review
    cat_perf = db.get_category_performance()
    category_reviews = []
    live_candidates = []

    for cat in cat_perf:
        name = cat.get("category", "?")
        fired = cat.get("signals_fired", 0)
        resolved = cat.get("signals_resolved", 0)
        won = cat.get("signals_won", 0)
        wr = cat.get("win_rate")
        pnl = cat.get("total_pnl", 0)

        status = "testing"
        recommendation = None

        if resolved >= 20:
            if wr is not None and wr < 0.45:
                status = "underperforming"
                recommendation = "DEMOTE — consider reducing allocation"
            elif wr is not None and wr > 0.58 and resolved >= 50:
                status = "live_candidate"
                recommendation = "PROMOTE TO LIVE candidate — requires human approval"
                live_candidates.append(name)
        elif resolved < 5:
            status = "insufficient_data"

        category_reviews.append({
            "category": name,
            "signals_fired": fired,
            "signals_resolved": resolved,
            "signals_won": won,
            "win_rate": round(wr, 3) if wr is not None else None,
            "total_pnl": round(pnl, 2) if pnl else 0,
            "status": status,
            "recommendation": recommendation,
        })

    # 2. Wallet quality review
    with db._lock:
        demoted_count = db._conn.execute(
            """SELECT COUNT(*) FROM leaderboard
               WHERE rolling_20_wr IS NOT NULL AND rolling_20_wr < 0.45"""
        ).fetchone()[0]

        tier1_count = db._conn.execute(
            "SELECT COUNT(*) FROM leaderboard WHERE tier = 'tier1'"
        ).fetchone()[0]

        tier2_count = db._conn.execute(
            "SELECT COUNT(*) FROM leaderboard WHERE tier = 'tier2'"
        ).fetchone()[0]

    wallet_changes = {
        "tier1_count": tier1_count,
        "tier2_count": tier2_count,
        "demoted_by_rolling_wr": demoted_count,
    }

    # 3. Determine if action required
    action_required = bool(live_candidates) or any(
        c["status"] == "underperforming" for c in category_reviews
    )

    result = {
        "reviewed_at": int(time.time()),
        "categories": category_reviews,
        "wallet_changes": wallet_changes,
        "live_candidates": live_candidates,
        "action_required": action_required,
    }

    # Write report
    report_path = Path("data/polymarket/performance_review.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    # Print report
    print()
    print("Category Performance Review")
    print(f"{'Category':<14} | {'Fired':>6} | {'Resolved':>8} | {'WR':>6} | {'P&L':>10} | Status")
    print(f"{'-'*14}-+-{'-'*6}-+-{'-'*8}-+-{'-'*6}-+-{'-'*10}-+-{'-'*20}")
    for c in category_reviews:
        wr_str = f"{c['win_rate']:.0%}" if c["win_rate"] is not None else "—"
        pnl_str = f"${c['total_pnl']:,.0f}" if c["total_pnl"] else "—"
        print(f"{c['category']:<14} | {c['signals_fired']:>6} | {c['signals_resolved']:>8} | {wr_str:>6} | {pnl_str:>10} | {c['status']}")
    print()

    print(f"Wallet Quality: tier1={tier1_count} tier2={tier2_count}")
    print()

    if action_required:
        print("=" * 60)
        print("  ACTION REQUIRED — HUMAN REVIEW NEEDED")
        if live_candidates:
            print(f"  Live candidates: {', '.join(live_candidates)}")
        for c in category_reviews:
            if c["recommendation"]:
                print(f"  {c['category']}: {c['recommendation']}")
        print("=" * 60)
    else:
        print("No action required — all categories in testing phase.")

    return result
