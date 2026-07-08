"""Learned per-subcategory decay curves for resolution_decay (roadmap A2).

Replaces intuition with measurement: the hand-coded confidence
(0.50 + 0.20·time + 0.15·price) is self-inconsistent past 24h and encodes
no market evidence. This module fits an empirical
(slice × hours-bucket × price-bucket → resolve-YES rate) lookup from the
signal's own resolved history, shrunk toward the parent level (subcategory
→ category → global) because per-bucket n is tiny.

ROLLOUT (mandatory champion/challenger): the formula stays champion; the
lookup value rides along in the signal payload (decay_lookup_p → lands in
features_at_fire) until it beats the formula on held-out Brier (N4 gate).
DECAY_CURVE_ENFORCE=1 flips the lookup to champion. Expect the flip to
collapse live throughput: empirical yes-rates are 0.11-0.23 vs hand-coded
0.50-0.85, so the calibrated-EV gate becomes real.

Run: python -m trading_platform.polymarket.decay_curve   (scheduled daily)
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

HOURS_BUCKETS = (("01-03h", 1, 3), ("03-06h", 3, 6), ("06-12h", 6, 12),
                 ("12-24h", 12, 24), ("24-48h", 24, 48))
PRICE_BUCKETS = (("0.05-0.10", 0.05, 0.10), ("0.10-0.20", 0.10, 0.20),
                 ("0.20-0.30", 0.20, 0.30), ("0.30-0.40", 0.30, 0.40))
SHRINK_K = 20
MIN_CELL_MARKETS = 30
LOOKUP_MAX_AGE_SEC = 7 * 86400
GLOBAL_KEY = "__global__"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS decay_curve_lookup (
    slice_key    TEXT NOT NULL,
    level        TEXT NOT NULL,
    hours_bucket TEXT NOT NULL,
    price_bucket TEXT NOT NULL,
    n_markets    INTEGER NOT NULL,
    n_yes        INTEGER NOT NULL,
    p_shrunk     REAL NOT NULL,
    prior        REAL NOT NULL,
    window_days  INTEGER NOT NULL,
    updated_at   BIGINT NOT NULL,
    PRIMARY KEY (slice_key, hours_bucket, price_bucket)
);
"""


def _bucket_hours(hours: float) -> str | None:
    for name, lo, hi in HOURS_BUCKETS:
        if lo <= hours < hi:
            return name
    return None


def _bucket_price(price: float) -> str | None:
    for name, lo, hi in PRICE_BUCKETS:
        if lo <= price < hi:
            return name
    return None


def shrink(n_yes: int, n: int, prior: float, k: int = SHRINK_K) -> float:
    """Beta-binomial style shrink toward the parent level's rate."""
    return (n_yes + k * prior) / (n + k) if (n + k) > 0 else prior


def fit_decay_lookup(window_days: int = 90, k: int = SHRINK_K,
                     db_path: str | None = None) -> dict[str, Any]:
    """Fit the 3-level lookup from resolved resolution_decay history.

    n per cell counts DISTINCT markets (a market firing 20× inside one cell
    is one event — G3 clustering). Labels come from the canonical
    market_resolutions table (P1); rows without a canonical label are
    skipped (never guess a training label).
    """
    now = int(time.time())
    cutoff = now - window_days * 86400
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        for stmt in _SCHEMA.split(";"):
            if stmt.strip():
                try: conn.execute(stmt)
                except Exception: pass
        rows = conn.execute(
            """SELECT DISTINCT lt.condition_id, lt.signal_fired_at,
                      lt.resolution_date, lt.signal_price,
                      COALESCE(lt.category, 'other'), m.subcategory
                 FROM live_trades lt
                 LEFT JOIN markets m ON m.condition_id = lt.condition_id
                WHERE lt.signal_type = 'resolution_decay'
                  AND lt.signal_fired_at IS NOT NULL
                  AND lt.resolution_date IS NOT NULL
                  AND lt.signal_price IS NOT NULL
                  AND lt.signal_fired_at > ?""",
            (cutoff,),
        ).fetchall()

        # Canonical labels (P1). One bulk read.
        from trading_platform.polymarket.resolutions import get_resolutions_bulk
        labels = {cid: r["resolves_yes"] for cid, r in get_resolutions_bulk(
            [r[0] for r in rows if r[0]],
            db_path=db_path).items() if r["resolves_yes"] is not None}

        # cell → {cid: label} so a market counts once per cell.
        cells: dict[tuple, dict[str, int]] = {}
        n_labeled = 0
        for cid, fired_at, res_date, price, cat, subcat in rows:
            label = labels.get((cid or "").lower())
            if label is None:
                continue
            hours = (float(res_date) - float(fired_at)) / 3600.0
            hb = _bucket_hours(hours)
            pb = _bucket_price(float(price))
            if hb is None or pb is None:
                continue
            n_labeled += 1
            cat_l = (cat or "other").lower()
            sub_l = (subcat or "").lower() or None
            for slice_key, level in ((GLOBAL_KEY, "global"),
                                     (cat_l, "category"),
                                     (sub_l, "subcategory")):
                if slice_key is None:
                    continue
                cells.setdefault((slice_key, level, hb, pb), {})[cid] = int(label)

        # Fit levels top-down so children shrink toward fitted parents.
        p_global_all = 0.5
        g_labels = [v for (sk, lvl, _, _), d in cells.items()
                    if lvl == "global" for v in d.values()]
        if g_labels:
            p_global_all = sum(g_labels) / len(g_labels)

        fitted: dict[tuple, tuple] = {}
        for level in ("global", "category", "subcategory"):
            for (sk, lvl, hb, pb), d in cells.items():
                if lvl != level:
                    continue
                n, n_yes = len(d), sum(d.values())
                if level == "global":
                    prior = p_global_all
                elif level == "category":
                    prior = fitted.get((GLOBAL_KEY, "global", hb, pb),
                                       (None, p_global_all))[1]
                else:
                    # subcategory shrinks toward its category cell; the
                    # category of a subcategory row: parent = the category
                    # cell with the same buckets that contains these cids —
                    # approximate with the global-cell prior when absent.
                    prior = None
                    for (sk2, lvl2, hb2, pb2), _ in cells.items():
                        if (lvl2 == "category" and hb2 == hb and pb2 == pb
                                and sk.startswith(sk2)):
                            prior = fitted.get((sk2, "category", hb, pb),
                                               (None, None))[1]
                            break
                    if prior is None:
                        prior = fitted.get((GLOBAL_KEY, "global", hb, pb),
                                           (None, p_global_all))[1]
                fitted[(sk, lvl, hb, pb)] = (n, shrink(n_yes, n, prior, k),
                                             prior, n_yes)

        conn.execute("DELETE FROM decay_curve_lookup")
        for (sk, lvl, hb, pb), (n, p, prior, n_yes) in fitted.items():
            conn.execute(
                """INSERT INTO decay_curve_lookup
                     (slice_key, level, hours_bucket, price_bucket,
                      n_markets, n_yes, p_shrunk, prior, window_days,
                      updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (sk, lvl, hb, pb, n, n_yes, round(p, 4), round(prior, 4),
                 window_days, now))
        conn.commit()
        return {"cells": len(fitted), "observations": n_labeled,
                "labeled_markets": len(labels), "p_global": round(p_global_all, 4)}
    finally:
        try: conn.close()
        except Exception: pass


def lookup_probability(subcategory: str | None, category: str | None,
                       hours: float, price: float,
                       db_path: str | None = None) -> float | None:
    """Fitted resolve-YES probability for a candidate, or None (caller keeps
    the formula). Waterfall: subcategory cell (n>=30) → category cell
    (n>=30) → global cell; stale table (>7d) → None."""
    hb, pb = _bucket_hours(hours), _bucket_price(price)
    if hb is None or pb is None:
        return None
    now = int(time.time())
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        for sk, need_n in (((subcategory or "").lower() or None, MIN_CELL_MARKETS),
                           ((category or "").lower() or None, MIN_CELL_MARKETS),
                           (GLOBAL_KEY, 1)):
            if not sk:
                continue
            try:
                r = conn.execute(
                    """SELECT p_shrunk, n_markets, updated_at
                         FROM decay_curve_lookup
                        WHERE slice_key = ? AND hours_bucket = ?
                          AND price_bucket = ?""",
                    (sk, hb, pb),
                ).fetchone()
            except Exception:
                return None
            if not r:
                continue
            p, n, updated_at = float(r[0]), int(r[1] or 0), int(r[2] or 0)
            if now - updated_at > LOOKUP_MAX_AGE_SEC:
                return None
            if n >= need_n:
                return max(0.05, min(0.90, p))
        return None
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    out = fit_decay_lookup()
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
