"""Naive-copy BUY signal — experimental lane.

Tests the hypothesis that copying the high-WR cohort with $5/trade produces
the +13.7% / 47d ROI seen in the backtested per-dollar baseline.

Cohort: wallet_profiles where resolved_trades >= 100 AND win_rate >= 0.60
        AND last_trade_ts > now - 30d, top 50 by net_pnl_usdc.

For each new cohort BUY:
  * filter to entries in last 15min (so we'd actually have caught it live)
  * filter price 0.10..0.85, horizon 6h..720h, exclude science category
  * dedup: same (wallet, condition_id) not copied twice
  * cap: at most NAIVE_COPY_MAX_OPEN open shadow positions
  * shadow insert into live_trades with dry_run=1, signal_type='naive_copy_buy'
  * record both the whale's price (entry_price) and our hypothetical fill
    (current best-ask, captured as expected_price) so slippage can be
    measured retrospectively

The existing resolution pipeline (signal_resolver + zero-balance handling)
will populate realized_pnl when the market resolves. After ~30 resolved
shadow trades, evaluate vs the +13.7% baseline. If it holds, flip to live
with a hard budget cap by setting NAIVE_COPY_LIVE_ENABLED=1.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────
NAIVE_COPY_MAX_OPEN = int(os.environ.get("NAIVE_COPY_MAX_OPEN", "10"))
NAIVE_COPY_STAKE_USD = float(os.environ.get("NAIVE_COPY_STAKE_USD", "5.0"))
NAIVE_COPY_LOOKBACK_MIN = int(os.environ.get("NAIVE_COPY_LOOKBACK_MIN", "75"))
NAIVE_COPY_LIVE_ENABLED = os.environ.get("NAIVE_COPY_LIVE_ENABLED", "0") == "1"
NAIVE_COPY_PER_CYCLE_CAP = int(os.environ.get("NAIVE_COPY_PER_CYCLE_CAP", "5"))

BLOCKED_CATEGORIES = {"science"}  # 0% WR in our existing pipeline
PRICE_MIN = 0.10
PRICE_MAX = 0.85
HORIZON_MIN_HOURS = 6
HORIZON_MAX_HOURS = 720  # 30 days


def _get_cohort(conn) -> list[str]:
    """Return the top-50 cohort by rolling-window quality (wq_score).

    Replaces the lifetime-PnL ranking that flagged wallets with $4M
    lifetime PnL but −36% in the 47-day test window. wq_score combines
    60d WR + sample size + recency + size consistency. See wallet_quality.py.
    """
    rows = conn.execute(
        """
        SELECT wallet FROM wallet_profiles
         WHERE wq_score IS NOT NULL
           AND wq_n_60d >= 30
           AND wq_wr_60d >= 0.55
           AND wq_recency_days <= 30
         ORDER BY wq_score DESC NULLS LAST
         LIMIT 50
        """
    ).fetchall()
    return [r[0] for r in rows]


def _open_shadow_count(conn) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) FROM live_trades
        WHERE signal_type = 'naive_copy_buy'
          AND dry_run = 1
          AND realized_pnl IS NULL
        """
    ).fetchone()
    return int(row[0] or 0)


def _already_copied(conn, wallet: str, condition_id: str) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM live_trades
        WHERE signal_type = 'naive_copy_buy'
          AND signal_wallet = %s
          AND condition_id = %s
        LIMIT 1
        """,
        (wallet, condition_id),
    ).fetchone()
    return bool(row)


def _market_info(conn, condition_id: str) -> dict | None:
    """Return {category, end_ts, yes_token_id, no_token_id} or None."""
    row = conn.execute(
        """
        SELECT m.end_date_iso, m.yes_token_id, m.no_token_id, mc.category
        FROM markets m
        LEFT JOIN market_categories mc ON mc.condition_id = m.condition_id
        WHERE m.condition_id = %s
        """,
        (condition_id,),
    ).fetchone()
    if not row:
        return None
    # end_date_iso may be a timestamp/string — parse to epoch seconds
    end_ts: int | None = None
    raw_end = row[0]
    if raw_end is not None:
        try:
            if hasattr(raw_end, "timestamp"):
                end_ts = int(raw_end.timestamp())
            else:
                import datetime as _dt
                end_ts = int(_dt.datetime.fromisoformat(str(raw_end).replace("Z", "+00:00")).timestamp())
        except Exception:
            end_ts = None
    return {
        "category": row[3],
        "end_ts": end_ts,
        "yes_token_id": row[1],
        "no_token_id": row[2],
    }


def _get_current_ask(token_id: str) -> float | None:
    """Best ask price for THIS token — must be in the token's own price space.

    /book returns asks in YES-space regardless of which token_id you ask. So
    for a NO token bought at e.g. 0.18, /book returns the corresponding YES
    asks at ~0.82. To get the NO-side ask correctly: take YES_bids[0].price
    (top of YES bid = best YES seller's bid → 1 - that = best NO ask).
    """
    try:
        from trading_platform.polymarket.clob_client import ClobClient
        book = ClobClient().get_order_book(token_id)
        # Always interpret as YES-space, then we don't know whether this
        # token IS YES or NO. Caller passes the whale's price as a sanity
        # anchor — if the YES-space ask is wildly off the whale's, this
        # token is NO and we should use 1 - YES_bid.
        asks = book.get("asks") or []
        if asks:
            return float(asks[0]["price"])
    except Exception as exc:
        logger.debug("ask lookup failed for %s: %s", token_id[:12], exc)
    return None


def _estimated_fill_price(token_id: str, whale_price: float) -> float:
    """Direction-aware fill estimate. If the YES-space ask is more than 0.20
    from the whale's price, we're looking at the wrong side — invert."""
    yes_space_ask = _get_current_ask(token_id)
    if yes_space_ask is None:
        return whale_price
    if abs(yes_space_ask - whale_price) <= 0.20:
        return yes_space_ask  # this token IS YES (or close enough)
    # This token is NO — back out the NO ask. /book gives YES asks; NO ask
    # = 1 - YES_bid. We don't have YES_bid here; approximate as 1 - YES_ask
    # (tighter approximation than nothing; spread is usually 1-2¢).
    return max(0.01, min(0.99, 1.0 - yes_space_ask))


def _find_candidate_trades(conn, cohort: list[str]) -> list[dict[str, Any]]:
    """Recent BUYs by cohort in the last NAIVE_COPY_LOOKBACK_MIN minutes."""
    if not cohort:
        return []
    cutoff = int(time.time()) - NAIVE_COPY_LOOKBACK_MIN * 60
    placeholders = ",".join(["%s"] * len(cohort))
    rows = conn.execute(
        f"""
        SELECT wallet, condition_id, price, size, timestamp, asset
        FROM wallet_trades
        WHERE wallet IN ({placeholders})
          AND side = 'BUY'
          AND timestamp >= %s
          AND price > 0
          AND condition_id IS NOT NULL
        ORDER BY timestamp ASC
        """,
        tuple(cohort) + (cutoff,),
    ).fetchall()
    return [
        {
            "wallet": r[0],
            "condition_id": r[1],
            "whale_price": float(r[2]),
            "whale_size": float(r[3] or 0),
            "ts": int(r[4]),
            "asset": r[5],
        }
        for r in rows
    ]


def run_cycle() -> dict[str, int]:
    """One scan-and-insert pass. Returns counters."""
    stats = {
        "candidates": 0,
        "shadow_inserts": 0,
        "skipped_dedup": 0,
        "skipped_price": 0,
        "skipped_horizon": 0,
        "skipped_category": 0,
        "skipped_cap": 0,
        "skipped_no_market": 0,
    }
    conn = get_connection()
    try:
        cohort = _get_cohort(conn)
        if not cohort:
            logger.warning("[naive_copy] empty cohort — skipping")
            return stats

        open_count = _open_shadow_count(conn)
        room = max(0, NAIVE_COPY_MAX_OPEN - open_count)
        if room == 0:
            logger.info("[naive_copy] at open-position cap (%d)", NAIVE_COPY_MAX_OPEN)
            return stats

        candidates = _find_candidate_trades(conn, cohort)
        stats["candidates"] = len(candidates)

        inserted = 0
        for c in candidates:
            if inserted >= NAIVE_COPY_PER_CYCLE_CAP or inserted >= room:
                stats["skipped_cap"] += 1
                continue
            if not (PRICE_MIN <= c["whale_price"] <= PRICE_MAX):
                stats["skipped_price"] += 1
                continue
            if _already_copied(conn, c["wallet"], c["condition_id"]):
                stats["skipped_dedup"] += 1
                continue
            mkt = _market_info(conn, c["condition_id"]) or {}
            # Horizon is a soft filter — if end_ts unknown, allow (we'll
            # post-hoc filter when evaluating). When known, enforce bounds.
            end_ts = mkt.get("end_ts")
            if end_ts:
                horizon_h = (end_ts - time.time()) / 3600.0
                if not (HORIZON_MIN_HOURS <= horizon_h <= HORIZON_MAX_HOURS):
                    stats["skipped_horizon"] += 1
                    continue
            else:
                horizon_h = float("nan")
            if (mkt.get("category") or "").lower() in BLOCKED_CATEGORIES:
                stats["skipped_category"] += 1
                continue

            # We know the exact token from the whale's trade — no need to
            # look up YES/NO from markets. asset IS the token_id.
            token_id = c["asset"] or mkt.get("yes_token_id")
            if not token_id:
                stats["skipped_no_market"] += 1
                continue
            # Shadow trades assume perfect fill at whale's price — this is
            # the same assumption as the +13.7%/47d baseline. When the lane
            # is promoted live (NAIVE_COPY_LIVE_ENABLED=1), actual CLOB
            # fills replace this and slippage becomes observable through
            # the normal trade_fills pipeline.
            entry_price = c["whale_price"]
            expected = c["whale_price"]
            shares = NAIVE_COPY_STAKE_USD / max(entry_price, 0.01)

            conn.execute(
                """
                INSERT INTO live_trades (
                    attempted_at, signal_type, condition_id, question,
                    direction, side, confidence, size_usd, entry_price,
                    expected_price, slippage, shares, token_id, category,
                    status, dry_run, signal_wallet
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
                """,
                (
                    int(time.time()), "naive_copy_buy", c["condition_id"], None,
                    "BUY", "BUY", 0.60, NAIVE_COPY_STAKE_USD, entry_price,
                    expected, expected - entry_price, shares, token_id, mkt.get("category"),
                    "matched", 0 if NAIVE_COPY_LIVE_ENABLED else 1, c["wallet"],
                ),
            )
            conn.commit()
            inserted += 1
            stats["shadow_inserts"] += 1
            logger.info(
                "[naive_copy] shadow BUY wallet=%s cid=%s... whale_px=%.3f our_px=%.3f horizon=%.1fh",
                c["wallet"][:10], c["condition_id"][:12], entry_price, expected, horizon_h,
            )

        return stats
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    stats = run_cycle()
    logger.info(
        "[naive_copy] cycle done: candidates=%d shadow=%d dedup=%d price=%d horizon=%d category=%d cap=%d no_market=%d (live=%d)",
        stats["candidates"], stats["shadow_inserts"],
        stats["skipped_dedup"], stats["skipped_price"], stats["skipped_horizon"],
        stats["skipped_category"], stats["skipped_cap"], stats["skipped_no_market"],
        int(NAIVE_COPY_LIVE_ENABLED),
    )


if __name__ == "__main__":
    main()
