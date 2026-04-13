"""
Build the wallet leaderboard from profiles and trade data.

Ranks wallets by conviction score, assigns tier1/tier2, applies
rolling WR demotion, and writes atomically to the leaderboard table.

Usage::

    from trading_platform.polymarket.leaderboard import build_leaderboard
    result = build_leaderboard()
"""
from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any

from trading_platform.polymarket.wallet_db import WalletDB

logger = logging.getLogger(__name__)

TIER1_MIN_WR = 0.58
TIER1_MIN_RESOLVED = 25
TIER1_MIN_VOLUME = 5_000
TIER1_MIN_PROFIT_FACTOR = 1.3
TIER2_MIN_WR = 0.53
TIER2_MIN_RESOLVED = 5
TIER2_MIN_VOLUME = 1_000
DEMOTION_ROLLING_WR = 0.45

# High-conviction tier — captures top Polymarket whales who make
# few but extremely large profitable bets
TIER1H_MIN_PNL = 500_000
TIER1H_MIN_AVG_TRADE = 5_000
TIER1H_MIN_WR = 0.50
TIER1H_MIN_RESOLVED = 3


def _compute_conviction_score(profile: dict[str, Any]) -> float:
    """Multi-factor conviction score using analytics metrics.

    Combines: WR × log(trades), EV bonus, Sharpe bonus, trend multiplier,
    recency multiplier, specialization bonus, drawdown penalty.
    """
    import math as _math
    score = 0.0

    wr = profile.get("directional_win_rate") or 0
    n = profile.get("resolved_trades") or 0
    if n >= 5:
        log_n = _math.log10(max(n, 1))
        score += wr * log_n * 2  # max ~6 at 100% WR / 1000 trades

    ev = profile.get("expected_value")
    if ev is not None and ev > 0:
        score += min(ev / 100, 2.0)

    sharpe = profile.get("sharpe_ratio")
    if sharpe is not None and sharpe > 0:
        score += min(sharpe * 0.5, 1.5)

    trend = profile.get("pnl_trend")
    if trend is not None:
        if trend > 1.5:
            score *= 1.2
        elif trend > 1.0:
            score *= 1.1
        elif trend < 0:
            score *= 0.7
        elif trend < 0.5:
            score *= 0.85

    last_ts = profile.get("last_trade_ts") or 0
    if last_ts:
        days_inactive = (time.time() - last_ts) / 86400
        if days_inactive < 7:
            pass
        elif days_inactive < 30:
            score *= 0.9
        elif days_inactive < 90:
            score *= 0.75
        else:
            score *= 0.5

    spec = profile.get("category_specialization")
    if spec is not None and spec > 0.2:
        score += min(spec, 0.5)

    mdd = profile.get("max_drawdown_pct")
    if mdd is not None and mdd > 0.5:
        score *= max(0.7, 1 - (mdd - 0.5))

    return round(max(score, 0), 3)


def build_leaderboard(db: WalletDB | None = None) -> dict[str, Any]:
    """Rebuild leaderboard from wallet_profiles + wallet_trades.

    Only commits if at least 1 wallet qualifies — never overwrites
    a valid leaderboard with an empty one.
    """
    db = db or WalletDB()
    t0 = time.time()
    version = db.get_leaderboard_version() + 1
    db.begin_leaderboard_rebuild(version)

    conn = sqlite3.connect(str(db._path))
    rows = conn.execute("""
        SELECT wallet, directional_win_rate, conviction_score,
               resolved_trades, total_volume_usdc, wallet_type,
               last_trade_ts, profit_factor, category_trades,
               net_pnl_usdc, avg_position_size_usdc,
               sharpe_ratio, expected_value, pnl_trend,
               category_specialization, max_drawdown_pct
        FROM wallet_profiles
        WHERE directional_win_rate IS NOT NULL
    """).fetchall()
    conn.close()

    tier1h_list: list[str] = []
    tier1_list: list[str] = []
    tier2_list: list[str] = []
    demoted = 0
    insufficient = 0
    leaderboard_rows: list[dict[str, Any]] = []
    cat_dist: dict[str, int] = {}

    for row in rows:
        (wallet, wr, conv, resolved, volume, wtype, last_ts, pf, cat_json,
         net_pnl, avg_size, sharpe, ev, trend, spec, mdd) = row
        resolved = resolved or 0
        volume = volume or 0
        net_pnl = net_pnl or 0
        avg_size = avg_size or 0
        # pf may be None (insufficient losses) — treat as "passes the gate"

        # Compute smart conviction score from analytics
        smart_conv = _compute_conviction_score({
            "directional_win_rate": wr or 0,
            "resolved_trades": resolved,
            "expected_value": ev,
            "sharpe_ratio": sharpe,
            "pnl_trend": trend,
            "last_trade_ts": last_ts,
            "category_specialization": spec,
            "max_drawdown_pct": mdd,
        })
        # Use smart conviction if computable, else fall back to legacy
        conv = smart_conv if smart_conv > 0 else (conv or 0)

        # Primary category from category_trades — normalize and exclude noise
        primary_category = "unknown"
        try:
            import json as _json
            ct = _json.loads(cat_json) if cat_json else {}
            if ct:
                # Map old names to new 9-category system
                _CAT_MAP = {
                    "tweet_count": "mentions", "other": "other", "sports": "sports",
                    "politics": "politics", "crypto": "crypto", "economics": "economics",
                    "finance": "finance", "tech": "tech", "culture": "culture",
                }
                normalized: dict[str, int] = {}
                for k, v in ct.items():
                    mapped = _CAT_MAP.get((k or "").lower(), "other")
                    normalized[mapped] = normalized.get(mapped, 0) + (v or 0)
                # Prefer meaningful categories (exclude generic "other" and noisy "mentions")
                meaningful = {k: v for k, v in normalized.items() if k not in ("other", "mentions")}
                if meaningful:
                    primary_category = max(meaningful, key=meaningful.get)
                elif normalized:
                    primary_category = max(normalized, key=normalized.get)
        except Exception:
            pass

        # Demotion check via rolling window
        rolling = db.compute_rolling_wr(wallet, n=20)
        if rolling is not None and rolling < DEMOTION_ROLLING_WR:
            demoted += 1
            continue

        # Tier assignment — check tier1h first (high-conviction whales)
        # then tier1 (statistical 25+ resolved), then tier2
        pf_ok = (pf is None) or (pf >= TIER1_MIN_PROFIT_FACTOR)

        is_tier1h = (
            net_pnl >= TIER1H_MIN_PNL
            and avg_size >= TIER1H_MIN_AVG_TRADE
            and wr >= TIER1H_MIN_WR
            and resolved >= TIER1H_MIN_RESOLVED
        )
        is_tier1 = (
            wr >= TIER1_MIN_WR
            and resolved >= TIER1_MIN_RESOLVED
            and volume >= TIER1_MIN_VOLUME
            and pf_ok
        )
        is_tier2 = (
            wr >= TIER2_MIN_WR
            and resolved >= TIER2_MIN_RESOLVED
            and volume >= TIER2_MIN_VOLUME
        )

        if is_tier1h:
            tier = "tier1h"
            tier1h_list.append(wallet)
            cat_dist[primary_category] = cat_dist.get(primary_category, 0) + 1
        elif is_tier1:
            tier = "tier1"
            tier1_list.append(wallet)
            cat_dist[primary_category] = cat_dist.get(primary_category, 0) + 1
        elif is_tier2:
            tier = "tier2"
            tier2_list.append(wallet)
        else:
            insufficient += 1
            continue

        leaderboard_rows.append({
            "wallet": wallet,
            "tier": tier,
            "directional_win_rate": wr,
            "rolling_20_wr": rolling,
            "conviction_score": conv or 0.0,
            "resolved_trades": resolved,
            "total_volume_usdc": volume,
            "wallet_type": wtype,
            "last_trade_ts": last_ts,
            "profit_factor": round(pf, 3) if pf is not None else None,
            "primary_category": primary_category,
            "net_pnl_usdc": round(net_pnl, 2),
            "avg_position_size_usdc": round(avg_size, 2),
            "version": version,
            "updated_at": int(time.time()),
        })

    if not leaderboard_rows:
        logger.warning("Leaderboard build produced 0 wallets — keeping previous version")
        print("[WARNING] 0 wallets qualified — previous leaderboard preserved.")
        return {
            "version": version - 1,
            "tier1h": 0,
            "tier1": 0,
            "tier2": 0,
            "demoted": demoted,
            "insufficient": insufficient,
            "elapsed_seconds": round(time.time() - t0, 1),
            "committed": False,
        }

    # Rank within tier: tier1h first by PnL, then tier1/tier2 by conviction
    _TIER_ORDER = {"tier1h": 0, "tier1": 1, "tier2": 2}
    leaderboard_rows.sort(key=lambda r: (
        _TIER_ORDER.get(r["tier"], 3),
        -(r["net_pnl_usdc"] or 0) if r["tier"] == "tier1h" else -(r["conviction_score"] or 0),
    ))
    for i, r in enumerate(leaderboard_rows):
        r["rank"] = i + 1

    # Write to DB — migrate schema if needed
    conn = sqlite3.connect(str(db._path))
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(leaderboard)").fetchall()}
    for col, typedef in [
        ("profit_factor", "REAL"), ("primary_category", "TEXT"),
        ("net_pnl_usdc", "REAL"), ("avg_position_size_usdc", "REAL"),
        ("leaderboard_source", "TEXT"), ("pseudonym", "TEXT"),
    ]:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE leaderboard ADD COLUMN {col} {typedef}")

    # Preserve polymarket-sourced tier1h wallets across rebuilds
    pm_sourced = conn.execute(
        "SELECT wallet, tier, leaderboard_source, net_pnl_usdc, pseudonym, primary_category, rank FROM leaderboard WHERE leaderboard_source LIKE 'polymarket_%'"
    ).fetchall()
    pm_wallets = {r[0] for r in pm_sourced}

    conn.execute("DELETE FROM leaderboard")
    locally_inserted = set()
    for r in leaderboard_rows:
        r["leaderboard_source"] = "local"
        conn.execute("""
            INSERT OR REPLACE INTO leaderboard
                (wallet, tier, directional_win_rate, rolling_20_wr,
                 conviction_score, resolved_trades, total_volume_usdc,
                 wallet_type, last_trade_ts, rank, version, updated_at,
                 profit_factor, primary_category, net_pnl_usdc, avg_position_size_usdc,
                 leaderboard_source)
            VALUES
                (:wallet, :tier, :directional_win_rate, :rolling_20_wr,
                 :conviction_score, :resolved_trades, :total_volume_usdc,
                 :wallet_type, :last_trade_ts, :rank, :version, :updated_at,
                 :profit_factor, :primary_category, :net_pnl_usdc, :avg_position_size_usdc,
                 :leaderboard_source)
        """, r)
        locally_inserted.add(r["wallet"])

    # Restore polymarket-sourced wallets not already locally classified.
    # These keep their tier1h status from Polymarket's leaderboard.
    # ENRICHMENT: also compute WR from wallet_trades if available.
    pm_restored = 0
    pm_enriched = 0
    for pm_row in pm_sourced:
        pm_wallet, pm_tier, pm_src, pm_pnl, pm_name, pm_cat, pm_rank = pm_row
        if pm_wallet in locally_inserted:
            continue

        # Try to compute stats from wallet_trades
        pm_wr = None
        pm_resolved = None
        pm_volume = None
        pm_pf = None
        pm_rolling = None
        pm_wtype = None
        pm_category = pm_cat or "unknown"
        try:
            # Try reliable trades first, fall back to all trades
            stats = conn.execute("""
                SELECT COUNT(*) AS resolved,
                       SUM(CASE WHEN pnl > 0 THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS wr,
                       SUM(ABS(pnl * size)) AS volume,
                       SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) AS gross_win,
                       SUM(CASE WHEN pnl < 0 THEN -pnl ELSE 0 END) AS gross_loss
                FROM wallet_trades
                WHERE wallet = ? AND pnl IS NOT NULL AND pnl != 0 AND pnl_reliable = 1
            """, (pm_wallet,)).fetchone()
            # If no reliable trades, use all trades (estimated WR)
            if not stats or not stats[0] or stats[0] == 0:
                stats = conn.execute("""
                    SELECT COUNT(*) AS resolved,
                           SUM(CASE WHEN pnl > 0 THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS wr,
                           SUM(ABS(pnl * size)) AS volume,
                           SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) AS gross_win,
                           SUM(CASE WHEN pnl < 0 THEN -pnl ELSE 0 END) AS gross_loss
                    FROM wallet_trades
                    WHERE wallet = ? AND pnl IS NOT NULL AND pnl != 0
                """, (pm_wallet,)).fetchone()
            if stats and stats[0] and stats[0] > 0:
                pm_resolved = stats[0]
                pm_wr = round(stats[1], 4) if stats[1] else None
                pm_volume = round(stats[2] or 0, 2)
                gw, gl = stats[3] or 0, stats[4] or 0
                pm_pf = round(min(gw / gl, 99.0), 3) if gl > 0 else None
                pm_enriched += 1

                # Compute best category (use all trades, not just reliable)
                cat_row = conn.execute("""
                    SELECT category, COUNT(*) as n FROM wallet_trades
                    WHERE wallet = ? AND category IS NOT NULL
                      AND category NOT IN ('other', 'mentions')
                    GROUP BY category ORDER BY n DESC LIMIT 1
                """, (pm_wallet,)).fetchone()
                if cat_row:
                    pm_category = cat_row[0]
        except Exception:
            pass

        # Compute rolling WR
        try:
            pm_rolling = db.compute_rolling_wr(pm_wallet, n=20)
        except Exception:
            pass

        conn.execute("""
            INSERT OR REPLACE INTO leaderboard
                (wallet, tier, leaderboard_source, net_pnl_usdc, pseudonym,
                 primary_category, rank, version, updated_at,
                 directional_win_rate, rolling_20_wr, resolved_trades,
                 total_volume_usdc, profit_factor)
            VALUES (?, 'tier1h', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (pm_wallet, pm_src, pm_pnl, pm_name, pm_category,
              pm_rank, version, int(time.time()),
              pm_wr, pm_rolling, pm_resolved, pm_volume, pm_pf))
        pm_restored += 1

    if pm_restored:
        print(f"[LEADERBOARD] Restored {pm_restored} polymarket-sourced tier1h wallets ({pm_enriched} enriched with trade data)")
        tier1h_list.extend(["pm:" + str(i) for i in range(pm_restored)])
    # Sync wallet_bucket from leaderboard back to wallet_profiles
    conn.execute("""
        UPDATE wallet_profiles
        SET wallet_bucket = (
            SELECT wallet_bucket FROM leaderboard
            WHERE leaderboard.wallet = wallet_profiles.wallet
        )
        WHERE EXISTS (
            SELECT 1 FROM leaderboard WHERE leaderboard.wallet = wallet_profiles.wallet
        )
    """)
    conn.commit()
    conn.close()

    # tier1h wallets count toward tier1 in commit_leaderboard meta
    db.commit_leaderboard(version, len(tier1_list) + len(tier1h_list), len(tier2_list))

    elapsed = round(time.time() - t0, 1)

    # Log category distribution
    if cat_dist:
        dist_str = " | ".join(f"{k}: {v}" for k, v in sorted(cat_dist.items(), key=lambda x: -x[1]))
        print(f"[LEADERBOARD] Tier1+1h category distribution: {dist_str}")

    print(
        f"[LEADERBOARD v{version}] "
        f"tier1h={len(tier1h_list)} (high-conviction $500K+ PnL) "
        f"tier1={len(tier1_list)} (statistical 25+ resolved) "
        f"tier2={len(tier2_list)} "
        f"demoted={demoted} insufficient={insufficient} elapsed={elapsed}s"
    )

    if tier1h_list:
        print("  Top tier1h:")
        top1h = [r for r in leaderboard_rows if r["tier"] == "tier1h"][:5]
        for r in top1h:
            print(
                f"    {r['wallet'][:12]}... "
                f"PnL=${r['net_pnl_usdc']:,.0f} "
                f"WR={r['directional_win_rate']:.1%} "
                f"avg=${r['avg_position_size_usdc']:,.0f} "
                f"resolved={r['resolved_trades']}"
            )

    top5 = [r for r in leaderboard_rows if r["tier"] == "tier1"][:5]
    if top5:
        print("  Top tier1:")
        for r in top5:
            print(
                f"    {r['wallet'][:12]}... "
                f"WR={r['directional_win_rate']:.1%} "
                f"conv={r['conviction_score']:.3f} "
                f"resolved={r['resolved_trades']}"
            )

    return {
        "version": version,
        "tier1h": len(tier1h_list),
        "tier1": len(tier1_list),
        "tier2": len(tier2_list),
        "demoted": demoted,
        "insufficient": insufficient,
        "elapsed_seconds": elapsed,
        "committed": True,
    }
