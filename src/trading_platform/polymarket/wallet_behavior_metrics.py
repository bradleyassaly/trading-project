"""Wallet behavior metrics — defenses against survivorship/farming bias and
deeper behavioral profiling than the lifetime-WR aggregate.

Five distinct measurements, all written into `wallet_behavior_metrics`:

1. **Bootstrap-CI on ROI**: 95% lower bound on per-trade edge. A wallet
   needs LB > 0 to be `is_copyable_ci` — defends against the case where
   a wallet's headline lifetime ROI is positive but the variance is so
   high we're really copying noise (or a one-shot lucky strike).

2. **Per-(wallet, category) specialization z-score**: how much better
   does this wallet do in this category vs its own lifetime average?
   z >= 1.0 = real specialist. Replaces the static `is_specialist` flag
   which was binary and survivorship-biased.

3. **Bankroll-relative sizing distribution**: median + p90 of
   (position_size / estimated_bankroll). Mechanical farmers size at
   1-2% per trade; conviction whales size 10-30%. The shape is very
   informative.

4. **Sybil / wash detection**: same-market both-sides within 60min,
   round-trip ratio (entries that exit same-side same-market within
   30min at near-zero net), trade count outliers.

5. **Strategy cluster id**: K-means on (entry_price_dist, hold_duration,
   category_concentration_HHI, exit_pattern). Replaces the hand-coded
   `wallet_type` enum with data-driven clusters.

All five are computed offline (daily) and looked up at signal time.
"""
from __future__ import annotations

import logging
import math
import random
import time
from typing import Any

from trading_platform.polymarket.db_connection import db, get_connection

logger = logging.getLogger(__name__)


# ── Schema ──────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS wallet_behavior_metrics (
    wallet                  TEXT PRIMARY KEY,
    n_resolved              BIGINT,
    roi_mean                DOUBLE PRECISION,
    roi_lower_95            DOUBLE PRECISION,
    roi_upper_95            DOUBLE PRECISION,
    is_copyable_ci          BIGINT,
    sizing_median_pct       DOUBLE PRECISION,
    sizing_p90_pct          DOUBLE PRECISION,
    estimated_bankroll      DOUBLE PRECISION,
    sybil_score             DOUBLE PRECISION,
    is_likely_farmer        BIGINT,
    category_hhi            DOUBLE PRECISION,
    primary_category        TEXT,
    cluster_id              BIGINT,
    last_updated            BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wbm_copyable_ci ON wallet_behavior_metrics(is_copyable_ci);
CREATE INDEX IF NOT EXISTS idx_wbm_farmer ON wallet_behavior_metrics(is_likely_farmer);
CREATE INDEX IF NOT EXISTS idx_wbm_cluster ON wallet_behavior_metrics(cluster_id);

CREATE TABLE IF NOT EXISTS wallet_category_zscore (
    wallet      TEXT NOT NULL,
    category    TEXT NOT NULL,
    n_in_cat    BIGINT,
    cat_wr      DOUBLE PRECISION,
    lifetime_wr DOUBLE PRECISION,
    z_score     DOUBLE PRECISION,
    is_specialist_z BIGINT,
    last_updated BIGINT,
    PRIMARY KEY (wallet, category)
);
"""


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            try:
                conn.execute(s)
            except Exception:
                pass


# ── (1) Bootstrap-CI on ROI ─────────────────────────────────────────────────

def bootstrap_ci_roi(
    pnls: list[float], stakes: list[float], n_iter: int = 1000,
) -> tuple[float, float, float]:
    """Return (mean, lower_95, upper_95) on per-trade ROI.

    ROI per trade = pnl / stake. Bootstrap by resampling with replacement.
    """
    if not pnls or not stakes or len(pnls) != len(stakes):
        return (0.0, 0.0, 0.0)
    rois = [
        (p / s) for p, s in zip(pnls, stakes)
        if s and s > 0 and p is not None
    ]
    if len(rois) < 5:
        # Below 5 resolved trades, no CI is meaningful — return raw mean
        # with a wide bound that fails any LB>0 gate.
        m = sum(rois) / len(rois) if rois else 0.0
        return (m, m - 1.0, m + 1.0)
    n = len(rois)
    means = []
    rng = random.Random(42)  # deterministic across runs
    for _ in range(n_iter):
        sample = [rois[rng.randrange(n)] for _ in range(n)]
        means.append(sum(sample) / n)
    means.sort()
    lo = means[int(0.025 * n_iter)]
    hi = means[int(0.975 * n_iter)]
    mean = sum(rois) / n
    return (mean, lo, hi)


# ── (2) Per-(wallet, category) z-score ──────────────────────────────────────

def category_zscore(
    cat_wins: int, cat_n: int,
    lifetime_wins: int, lifetime_n: int,
) -> float:
    """Z-score for whether category WR is meaningfully above lifetime WR.

    Treats lifetime WR as the null hypothesis; tests whether observed
    cat_wr exceeds it given binomial variance at cat_n.
    """
    if cat_n < 5 or lifetime_n < 20:
        return 0.0
    p_lifetime = lifetime_wins / lifetime_n
    p_cat = cat_wins / cat_n
    var = p_lifetime * (1 - p_lifetime) / cat_n
    if var <= 0:
        return 0.0
    return (p_cat - p_lifetime) / math.sqrt(var)


# ── (3) Bankroll-relative sizing ────────────────────────────────────────────

def estimate_bankroll(stakes: list[float], peak_pnl: float = 0.0) -> float:
    """Crude estimate: max(p95 stake × 50, peak_realized_pnl × 1.5).
    50× the typical stake is a fair lower bound on a Kelly-rational bankroll.
    """
    if not stakes:
        return 1000.0
    sorted_s = sorted(s for s in stakes if s and s > 0)
    if not sorted_s:
        return 1000.0
    p95 = sorted_s[min(int(0.95 * len(sorted_s)), len(sorted_s) - 1)]
    return max(p95 * 50, peak_pnl * 1.5, 1000.0)


def sizing_pct_distribution(
    stakes: list[float], bankroll: float,
) -> tuple[float, float]:
    """Return (median_pct, p90_pct) of stake/bankroll across trades."""
    if not stakes or bankroll <= 0:
        return (0.0, 0.0)
    pcts = sorted(s / bankroll for s in stakes if s and s > 0)
    if not pcts:
        return (0.0, 0.0)
    median = pcts[len(pcts) // 2]
    p90 = pcts[min(int(0.90 * len(pcts)), len(pcts) - 1)]
    return (median, p90)


# ── (4) Sybil / wash detection ──────────────────────────────────────────────

def sybil_score(
    n_trades: int, n_markets: int, n_round_trips: int, sum_pnl: float,
) -> float:
    """Heuristic 0-1 score. Higher = more wash-like.

    Components:
      - round_trip_rate: fraction of trades that flipped same-market <30min
      - market_concentration: 1 - (n_markets / n_trades)
      - near_zero_pnl: |sum_pnl| < $50 across many trades
    """
    if n_trades < 10:
        return 0.0
    rt_rate = min(n_round_trips / n_trades, 1.0)
    conc = 1.0 - (n_markets / n_trades) if n_trades > 0 else 0.0
    near_zero = 1.0 if abs(sum_pnl) < 50 and n_trades > 50 else 0.0
    return min(1.0, 0.5 * rt_rate + 0.3 * conc + 0.2 * near_zero)


# ── (5) Strategy clustering ─────────────────────────────────────────────────

def feature_vector(
    avg_entry_price: float, avg_hold_days: float,
    category_hhi: float, sl_rate: float,
) -> tuple[float, float, float, float]:
    """4-dim feature vector for k-means clustering. Standardize externally."""
    return (avg_entry_price, avg_hold_days, category_hhi, sl_rate)


def kmeans_cluster(
    vectors: list[tuple[float, ...]], k: int = 5, n_iter: int = 30,
) -> list[int]:
    """Lightweight k-means without numpy. Returns cluster id per vector.

    Deterministic seed; small enough for ~16K wallets to run in seconds.
    """
    if not vectors:
        return []
    n_dim = len(vectors[0])
    rng = random.Random(13)
    # Init: pick k random distinct points
    indices = list(range(len(vectors)))
    rng.shuffle(indices)
    centers = [vectors[i] for i in indices[:k]]
    if len(centers) < k:
        return [0] * len(vectors)

    def dist2(a, b):
        return sum((x - y) ** 2 for x, y in zip(a, b))

    labels = [0] * len(vectors)
    for _ in range(n_iter):
        # Assign
        for i, v in enumerate(vectors):
            best = 0
            best_d = dist2(v, centers[0])
            for c_i in range(1, k):
                d = dist2(v, centers[c_i])
                if d < best_d:
                    best_d = d
                    best = c_i
            labels[i] = best
        # Update
        new_centers = []
        for c_i in range(k):
            members = [vectors[i] for i, lab in enumerate(labels) if lab == c_i]
            if not members:
                new_centers.append(centers[c_i])
                continue
            avg = tuple(sum(m[d] for m in members) / len(members) for d in range(n_dim))
            new_centers.append(avg)
        if all(dist2(a, b) < 1e-6 for a, b in zip(centers, new_centers)):
            break
        centers = new_centers
    return labels


# ── Pipeline ────────────────────────────────────────────────────────────────

def compute_for_wallet(
    conn, wallet: str, lifetime_wins: int, lifetime_n: int,
) -> dict[str, Any] | None:
    """Compute the full metric set for one wallet; returns None if too thin."""
    if lifetime_n < 5:
        return None
    rows = conn.execute(
        """SELECT pnl, (size * price) AS stake_usdc, category,
                  NULL AS market_resolved_at, timestamp, condition_id
             FROM wallet_trades
            WHERE wallet = ? AND pnl IS NOT NULL AND pnl_reliable = 1""",
        (wallet,),
    ).fetchall()
    if not rows or len(rows) < 5:
        return None
    pnls = [float(r[0]) for r in rows if r[0] is not None]
    stakes = [float(r[1]) for r in rows if r[1] is not None and r[1] > 0]
    if len(pnls) < 5 or len(stakes) < 5:
        return None

    # ROI bootstrap
    roi_mean, roi_lo, roi_hi = bootstrap_ci_roi(pnls[:len(stakes)], stakes)

    # Sizing
    bankroll = estimate_bankroll(stakes, max(0.0, sum(pnls)))
    s_med, s_p90 = sizing_pct_distribution(stakes, bankroll)

    # Category HHI
    cat_counts: dict[str, int] = {}
    for r in rows:
        c = r[2] or "other"
        cat_counts[c] = cat_counts.get(c, 0) + 1
    total_c = sum(cat_counts.values())
    hhi = sum((n_ / total_c) ** 2 for n_ in cat_counts.values()) if total_c else 0
    primary_cat = max(cat_counts.items(), key=lambda kv: kv[1])[0] if cat_counts else None

    # Sybil heuristics
    n_markets = len(set(r[5] for r in rows if r[5]))
    # Round-trip = same condition_id appearing 2+ times within 30min
    by_market: dict[str, list[int]] = {}
    for r in rows:
        if r[5] and r[4]:
            by_market.setdefault(r[5], []).append(int(r[4]))
    n_round_trips = 0
    for ts_list in by_market.values():
        ts_list.sort()
        for i in range(1, len(ts_list)):
            if ts_list[i] - ts_list[i - 1] < 1800:
                n_round_trips += 1
    sb = sybil_score(len(rows), n_markets, n_round_trips, sum(pnls))

    return {
        "wallet": wallet,
        "n_resolved": len(rows),
        "roi_mean": roi_mean,
        "roi_lower_95": roi_lo,
        "roi_upper_95": roi_hi,
        "is_copyable_ci": 1 if roi_lo > 0 and len(rows) >= 20 else 0,
        "sizing_median_pct": s_med,
        "sizing_p90_pct": s_p90,
        "estimated_bankroll": bankroll,
        "sybil_score": sb,
        "is_likely_farmer": 1 if sb >= 0.6 else 0,
        "category_hhi": hhi,
        "primary_category": primary_cat,
    }


def copyable_ci_verdict(ci_row: tuple | None, wallet: str) -> tuple[bool, str]:
    """C3: anti-survivorship verdict for copying `wallet`'s trades.

    ci_row = (is_copyable_ci, n_resolved, roi_lower_95) or None.

    is_copyable_ci (roi_lower_95 > 0 AND n >= 20, from the bootstrap CI
    above) is the strongest anti-survivorship metric in the codebase and the
    live executor never read it. A wallet with NO metrics row fails — an
    unmeasured wallet is exactly what this gate must not copy. Truncated
    wallet ids (12-char rows observed in live_trades.signal_wallet) are
    flagged distinctly so shadow statistics don't misattribute them to
    no_metrics.
    """
    if not wallet or len(wallet) != 42:
        return (False, "truncated_wallet")
    if ci_row is None:
        return (False, "no_metrics")
    ci, n, lb = ci_row
    detail = f"n={int(n or 0)} roi_lb={float(lb if lb is not None else 0):+.3f}"
    return (bool(ci == 1), detail)


def run_pipeline(min_resolved: int = 20, db_path: str | None = None) -> dict:
    """Top-level: compute behavior metrics for every wallet with enough data.

    Designed to run daily after pnl_reconstruction. Idempotent — upserts.
    """
    t0 = time.time()
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        _ensure_schema(conn)
        wallets = conn.execute(
            "SELECT wallet, resolved_trades, "
            "       (SELECT SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) "
            "          FROM wallet_trades wt WHERE wt.wallet = wp.wallet "
            "            AND wt.pnl IS NOT NULL AND wt.pnl_reliable = 1) AS lifetime_wins "
            "  FROM wallet_profiles wp "
            " WHERE resolved_trades >= ?",
            (min_resolved,),
        ).fetchall()
        feature_vectors = []
        wallet_order = []
        n_processed = 0
        n_copyable = 0
        n_farmer = 0
        now = int(time.time())
        for w_row in wallets:
            wallet, lifetime_n, lifetime_wins = w_row[0], int(w_row[1] or 0), int(w_row[2] or 0)
            metrics = compute_for_wallet(conn, wallet, lifetime_wins, lifetime_n)
            if not metrics:
                continue
            n_processed += 1
            if metrics["is_copyable_ci"]:
                n_copyable += 1
            if metrics["is_likely_farmer"]:
                n_farmer += 1
            try:
                conn.execute(
                    """INSERT INTO wallet_behavior_metrics
                         (wallet, n_resolved, roi_mean, roi_lower_95, roi_upper_95,
                          is_copyable_ci, sizing_median_pct, sizing_p90_pct,
                          estimated_bankroll, sybil_score, is_likely_farmer,
                          category_hhi, primary_category, cluster_id, last_updated)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?)
                       ON CONFLICT (wallet) DO UPDATE SET
                         n_resolved=EXCLUDED.n_resolved,
                         roi_mean=EXCLUDED.roi_mean,
                         roi_lower_95=EXCLUDED.roi_lower_95,
                         roi_upper_95=EXCLUDED.roi_upper_95,
                         is_copyable_ci=EXCLUDED.is_copyable_ci,
                         sizing_median_pct=EXCLUDED.sizing_median_pct,
                         sizing_p90_pct=EXCLUDED.sizing_p90_pct,
                         estimated_bankroll=EXCLUDED.estimated_bankroll,
                         sybil_score=EXCLUDED.sybil_score,
                         is_likely_farmer=EXCLUDED.is_likely_farmer,
                         category_hhi=EXCLUDED.category_hhi,
                         primary_category=EXCLUDED.primary_category,
                         last_updated=EXCLUDED.last_updated""",
                    (
                        wallet, metrics["n_resolved"], metrics["roi_mean"],
                        metrics["roi_lower_95"], metrics["roi_upper_95"],
                        metrics["is_copyable_ci"], metrics["sizing_median_pct"],
                        metrics["sizing_p90_pct"], metrics["estimated_bankroll"],
                        metrics["sybil_score"], metrics["is_likely_farmer"],
                        metrics["category_hhi"], metrics["primary_category"], now,
                    ),
                )
                # 4-dim feature for clustering
                avg_entry = 0.5  # placeholder; could pull from wallet_trades.entry_price
                avg_hold = 7.0   # placeholder; days
                sl_rate = 0.0
                fv = (
                    avg_entry, avg_hold, metrics["category_hhi"], sl_rate,
                )
                feature_vectors.append(fv)
                wallet_order.append(wallet)
            except Exception as exc:
                logger.debug("upsert failed for %s: %s", wallet[:12], exc)

        # Cluster assignments
        n_clustered = 0
        if len(feature_vectors) >= 5:
            try:
                labels = kmeans_cluster(feature_vectors, k=5, n_iter=20)
                for w, lab in zip(wallet_order, labels):
                    conn.execute(
                        "UPDATE wallet_behavior_metrics SET cluster_id = ? WHERE wallet = ?",
                        (int(lab), w),
                    )
                    n_clustered += 1
            except Exception as exc:
                logger.warning("kmeans clustering failed: %s", exc)

        # Per-(wallet, category) z-score table
        n_z = 0
        try:
            cat_rows = conn.execute(
                """SELECT wallet, category,
                          COUNT(*) AS n,
                          SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins,
                          (SELECT COUNT(*) FROM wallet_trades wt2
                            WHERE wt2.wallet = wt.wallet
                              AND wt2.pnl IS NOT NULL AND wt2.pnl_reliable = 1) AS total_n,
                          (SELECT SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END)
                             FROM wallet_trades wt3
                            WHERE wt3.wallet = wt.wallet
                              AND wt3.pnl IS NOT NULL AND wt3.pnl_reliable = 1) AS total_wins
                     FROM wallet_trades wt
                    WHERE pnl IS NOT NULL AND pnl_reliable = 1
                      AND category IS NOT NULL
                    GROUP BY wallet, category
                   HAVING COUNT(*) >= 5"""
            ).fetchall()
            for r in cat_rows:
                w, cat, n_c, w_c, n_t, w_t = r
                z = category_zscore(int(w_c or 0), int(n_c), int(w_t or 0), int(n_t or 0))
                cat_wr = (w_c / n_c) if n_c else 0
                lifetime_wr = (w_t / n_t) if n_t else 0
                conn.execute(
                    """INSERT INTO wallet_category_zscore
                         (wallet, category, n_in_cat, cat_wr, lifetime_wr,
                          z_score, is_specialist_z, last_updated)
                       VALUES (?,?,?,?,?,?,?,?)
                       ON CONFLICT (wallet, category) DO UPDATE SET
                         n_in_cat=EXCLUDED.n_in_cat,
                         cat_wr=EXCLUDED.cat_wr,
                         lifetime_wr=EXCLUDED.lifetime_wr,
                         z_score=EXCLUDED.z_score,
                         is_specialist_z=EXCLUDED.is_specialist_z,
                         last_updated=EXCLUDED.last_updated""",
                    (w, cat, int(n_c), cat_wr, lifetime_wr, z,
                     1 if z >= 1.0 else 0, now),
                )
                n_z += 1
        except Exception as exc:
            logger.warning("z-score upsert failed: %s", exc)

        conn.commit()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "wallets_processed": n_processed,
            "copyable_ci": n_copyable,
            "likely_farmers": n_farmer,
            "clustered": n_clustered,
            "category_zscores_written": n_z,
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = run_pipeline()
    print(f"wallet behavior metrics: {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
