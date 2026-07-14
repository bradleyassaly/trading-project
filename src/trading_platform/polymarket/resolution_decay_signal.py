"""Resolution-time decay signal — Phase B independent alpha source.

Hypothesis: Polymarket binary markets within 24h of resolution at
mid-low entry ($0.10 - $0.30) where on-chain order flow has dried up
tend to expire YES at higher rate than the entry price implies.

Why it's independent of the wallet-mirror lane:
  - No tracked-whale dependency: fires on time-to-resolve + price band
  - Most whales have already exited 24h before resolution, so the
    smart-money signal would be silent here
  - Decoupled alpha source diversifies the portfolio

How it fires:
  - Run every 15 min via scheduler
  - For each open market with end_date_iso ∈ [now+1h, now+24h]:
    * Pull yes_token_id mid-price from CLOB
    * If mid_price ∈ [0.10, 0.30] AND no recent whale activity,
      emit a SIGNAL with side=YES, conf computed from time-to-resolve
      and price band
  - Signal goes through the same paper executor stack — same gates,
    same calibration, same exit management

Entry confidence model:
  conf = 0.50 + 0.20 * (1 - hours_to_resolve / 24)
       + 0.15 * max(0, (0.20 - mid_price) / 0.10)

Behind feature flag PHASE_B_RESOLUTION_DECAY_ENABLED so it's a
paper-only experiment until validated. Disable: unset env var.
"""
from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


SIGNAL_TYPE = "resolution_decay"
ENV_FLAG = "PHASE_B_RESOLUTION_DECAY_ENABLED"
MIN_HOURS_TO_RESOLVE = 1.0
# 2026-04-27: widened 24h → 48h. Resolved-trade throughput is the
# binding constraint on L0→L1 promotion (per scale_up_roadmap).
# Markets resolving in 24-48h still produce a single-day-cycle
# resolution-data point with the corrected calibration.
MAX_HOURS_TO_RESOLVE = 48.0
# 2026-04-27: widened price band 0.10-0.30 → 0.05-0.40. Original
# was conservative; 0.05-0.10 long-shots have asymmetric upside +
# 0.30-0.40 mid-band has the same time-decay structure. Expected
# ~5-10× candidate volume.
ENTRY_PRICE_LOW = 0.05
ENTRY_PRICE_HIGH = 0.40
MIN_VOLUME_24H = 1000.0  # market must have any liquidity at all


def _enabled() -> bool:
    return os.environ.get(ENV_FLAG, "").lower() in ("1", "true", "yes")


def _api_post(url: str, payload: dict, timeout: float = 10.0) -> dict | None:
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url, data=data, method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.debug("api post failed (%s): %s", url, exc)
        return None


def _candidate_markets(conn) -> list[dict[str, Any]]:
    """Pull markets resolving within the next 24h with token ids set.

    2026-06-01: exclude cids that recently failed CLOB price-fetch.
    Audit showed 193 wasted attempts in 7d concentrated on 2 specific
    cids — likely markets that are in our local cache but CLOB
    doesn't have a tradeable price for (delisted / paused / pre-trading).
    The signal generator kept emitting them every 15-min cycle.
    Skip any cid that's had >= 2 'Could not fetch current price from CLOB'
    blocks in the past 24h.
    """
    now = time.time()
    failed_cids = set()
    try:
        cutoff_24h = int(now) - 86400
        bad_rows = conn.execute(
            """SELECT DISTINCT condition_id FROM live_trades
                WHERE attempted_at >= ?
                  AND error_msg LIKE ?
             GROUP BY condition_id HAVING COUNT(*) >= 2""",
            (cutoff_24h, '%Could not fetch current price%'),
        ).fetchall()
        failed_cids = {r[0] for r in bad_rows if r[0]}
        if failed_cids:
            logger.debug(
                "[resolution_decay] excluding %d cids with recent CLOB price-fetch failures",
                len(failed_cids),
            )
    except Exception:
        pass

    rows = conn.execute(
        """SELECT condition_id, slug, question, end_date_iso,
                  yes_token_id, no_token_id, outcome_prices, volume_24h,
                  subcategory, uma_status
             FROM markets
            WHERE end_date_iso IS NOT NULL
              AND end_date_iso > to_char(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS')
              -- 2026-07-08: 24h → 48h. MAX_HOURS_TO_RESOLVE was already 48
              -- but this SQL cap starved the Python filter of everything
              -- past 24h. The A2 decay table shows the strongest wedge in
              -- the 12-24h bucket (sports/0.10-0.20 +7.6c, n=36) — flow
              -- entering at 24-48h feeds those buckets; per-candidate
              -- gates (calibrated-EV, thin-book, decay challenger) still
              -- filter each fire.
              AND end_date_iso < to_char(
                    NOW() + interval '48 hours', 'YYYY-MM-DD"T"HH24:MI:SS')
              AND yes_token_id IS NOT NULL
              AND no_token_id IS NOT NULL"""
    ).fetchall()
    out = []
    for r in rows:
        cid, slug, q, end_iso, yes_tid, no_tid, prices_raw, vol, subcat, uma_status = r
        if cid in failed_cids:
            continue  # CLOB doesn't have a price for this market — skip
        try:
            from datetime import datetime, timezone
            clean = end_iso.replace("Z", "+00:00") if end_iso.endswith("Z") else end_iso
            end_dt = datetime.fromisoformat(clean)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            hours_to_resolve = (end_dt.timestamp() - now) / 3600
        except Exception:
            continue
        if not (MIN_HOURS_TO_RESOLVE <= hours_to_resolve <= MAX_HOURS_TO_RESOLVE):
            continue
        if vol is not None and float(vol) < MIN_VOLUME_24H:
            continue

        # outcome_prices is JSON like '["0.18", "0.82"]' — yes price first
        yes_price = None
        try:
            prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
            if isinstance(prices, list) and prices:
                yes_price = float(prices[0])
        except Exception:
            yes_price = None
        if yes_price is None or not (ENTRY_PRICE_LOW <= yes_price <= ENTRY_PRICE_HIGH):
            continue

        out.append({
            "condition_id": cid, "slug": slug or "", "question": q or "",
            "end_iso": end_iso, "yes_token_id": yes_tid, "no_token_id": no_tid,
            "yes_price": yes_price, "hours_to_resolve": hours_to_resolve,
            "volume_24h": float(vol or 0), "subcategory": subcat,
            # #6 Stage A (instrument-only): oracle state Gamma reports for
            # this market at fire time. The thesis names the exact edge —
            # "UMA proposed the outcome, the orderbook hasn't marked to 0/1
            # yet" — vs revert-risk when a market is disputed. We capture it
            # now (no decision change) so we can later measure whether it
            # separates decay winners from losers before gating on it.
            "uma_status": uma_status,
        })
    return out


def _confidence(hours_to_resolve: float, yes_price: float,
                subcategory: str | None = None,
                category: str | None = None) -> tuple[float, float | None]:
    """(confidence, decay_lookup_p) — champion/challenger (roadmap A2).

    Champion: the hand-coded formula (self-inconsistent past 24h, but it is
    what the live edge was validated on). Challenger: the fitted
    (slice × hours × price → resolve-YES rate) lookup, which rides along in
    the payload until it beats the formula on held-out Brier.
    DECAY_CURVE_ENFORCE=1 makes the lookup champion (fail-safe: formula on
    None). Expect the flip to collapse throughput — the calibrated-EV gate
    becomes real when confidence means P(YES).
    """
    time_lift = 0.20 * (1 - hours_to_resolve / 24.0)
    price_lift = 0.15 * max(0.0, (0.20 - yes_price) / 0.10)
    formula = min(0.85, max(0.10, 0.50 + time_lift + price_lift))
    lookup_p = None
    try:
        from trading_platform.polymarket.decay_curve import lookup_probability
        lookup_p = lookup_probability(subcategory, category,
                                      hours_to_resolve, yes_price)
    except Exception:
        lookup_p = None
    enforce = os.environ.get("DECAY_CURVE_ENFORCE", "0").lower() in ("1", "true", "yes")
    if enforce and lookup_p is not None:
        return lookup_p, lookup_p
    return formula, lookup_p


def _emit_signal(market: dict, api_url: str) -> None:
    """Insert into market_signals + signal_outcomes via API endpoint, OR
    direct DB write as a fallback. Mimics whale_signal_engine emission
    so the existing paper executor picks it up unchanged."""
    conn = get_connection()
    try:
        # A2: derive category ONCE with the same classifier the executor
        # uses, so fit-time and fire-time slice keys match.
        category = None
        try:
            from trading_platform.polymarket.market_categorizer import classify_keywords
            category, _src = classify_keywords(market.get("slug") or "",
                                               market.get("question") or "")
        except Exception:
            category = None
        conf, lookup_p = _confidence(market["hours_to_resolve"],
                                     market["yes_price"],
                                     subcategory=market.get("subcategory"),
                                     category=category)
        now_ts = int(time.time())
        # market_signals row — audit trail. (A2 fix: the previous INSERT
        # named columns entry_price/side/wallet_tier that do not exist in
        # market_signals; swallowed by the except, it NEVER landed a row.)
        try:
            conn.execute(
                """INSERT INTO market_signals
                     (signal_type, condition_id, fired_at, direction,
                      price, confidence, wallet, slug, question, category)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    SIGNAL_TYPE, market["condition_id"], now_ts, "BUY",
                    market["yes_price"], conf, "phase_b_resolution_decay",
                    market["slug"], market["question"][:200] or "", category,
                ),
            )
            conn.commit()
        except Exception as exc:
            logger.warning("market_signals insert failed: %s", exc)

        # Trigger executor by hitting the API endpoint that processes a
        # signal payload. Falls back to direct paper_executor if the
        # endpoint isn't available.
        payload = {
            "signal_type": SIGNAL_TYPE,
            "condition_id": market["condition_id"],
            "side": "YES",
            "direction": "BUY",
            "entry_price": market["yes_price"],
            "price": market["yes_price"],
            "confidence": conf,
            # A2 challenger: fitted decay-curve P(YES); rides into
            # features_at_fire for the champion/challenger Brier comparison.
            "decay_lookup_p": lookup_p,
            "category": category,
            "subcategory": market.get("subcategory"),
            "wallet": "phase_b_resolution_decay",
            "wallet_tier": "synthetic",
            "slug": market["slug"],
            "question": market["question"][:200],
            "yes_token_id": market["yes_token_id"],
            "no_token_id": market["no_token_id"],
            # N10: carry end_date_iso so the live executor's horizon gate is a
            # dict read and skips a Gamma round-trip on the hot path. The
            # candidate query already guarantees this is non-null and inside
            # [now, now+24h].
            "end_date_iso": market.get("end_iso"),
            # #6 Stage A: oracle state at fire time, snapshotted verbatim into
            # polymarket_paper_trades.features_at_fire (the executor dumps the
            # whole payload). Instrument-only — nothing reads this to gate a
            # trade yet. Measure first (does uma_status at fire predict a
            # better resolve-YES rate?), then promote to a gate behind a flag.
            "uma_status_at_fire": market.get("uma_status"),
            "fired_at": now_ts,
        }
        # Paper executor invocation
        try:
            from trading_platform.polymarket.polymarket_paper_executor import (
                PolymarketPaperExecutor,
            )
            executor = PolymarketPaperExecutor()
            paper_trade = executor.execute_signal(payload)
            if paper_trade:
                logger.info(
                    "[RESOLUTION_DECAY] PLACED %s @ %.3f conf=%.2f hours=%.1f",
                    market["slug"][:30], market["yes_price"],
                    conf, market["hours_to_resolve"],
                )
            else:
                logger.info(
                    "[RESOLUTION_DECAY] gated %s @ %.3f conf=%.2f",
                    market["slug"][:30], market["yes_price"], conf,
                )
        except Exception as exc:
            logger.debug("paper executor invocation failed: %s", exc)

        # Live executor invocation — mirrors whale_signal_engine routing
        try:
            from trading_platform.polymarket.polymarket_live_executor import (
                PolymarketLiveExecutor,
            )
            live_ex = PolymarketLiveExecutor()
            live_result = live_ex.execute(payload)
            if live_result and live_result.get("success"):
                logger.info(
                    "[RESOLUTION_DECAY][LIVE] submitted %s @ %.3f",
                    market["slug"][:30], market["yes_price"],
                )
            elif live_result:
                logger.debug(
                    "[RESOLUTION_DECAY][LIVE] gated: %s",
                    live_result.get("reason", ""),
                )
        except Exception as exc:
            logger.debug("live executor invocation failed: %s", exc)
    finally:
        try: conn.close()
        except Exception: pass


def run_pipeline(api_url: str = "http://localhost:8001") -> dict[str, Any]:
    if not _enabled():
        return {
            "skipped": f"{ENV_FLAG} not set",
            "elapsed_seconds": 0.0,
            "candidates": 0, "fired": 0,
        }
    t0 = time.time()
    conn = get_connection()
    try:
        candidates = _candidate_markets(conn)
    finally:
        try: conn.close()
        except Exception: pass

    fired = 0
    for m in candidates:
        try:
            _emit_signal(m, api_url)
            fired += 1
        except Exception as exc:
            logger.debug("emit failed for %s: %s", m.get("slug", "?"), exc)
    return {
        "elapsed_seconds": round(time.time() - t0, 1),
        "candidates": len(candidates),
        "fired": fired,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_pipeline())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
