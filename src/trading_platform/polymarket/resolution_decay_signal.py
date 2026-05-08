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
    """Pull markets resolving within the next 24h with token ids set."""
    now = time.time()
    rows = conn.execute(
        """SELECT condition_id, slug, question, end_date_iso,
                  yes_token_id, no_token_id, outcome_prices, volume_24h
             FROM markets
            WHERE end_date_iso IS NOT NULL
              AND end_date_iso > to_char(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS')
              AND end_date_iso < to_char(
                    NOW() + interval '24 hours', 'YYYY-MM-DD"T"HH24:MI:SS')
              AND yes_token_id IS NOT NULL
              AND no_token_id IS NOT NULL"""
    ).fetchall()
    out = []
    for r in rows:
        cid, slug, q, end_iso, yes_tid, no_tid, prices_raw, vol = r
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
        # Pull market subcategory so the signal payload + executor
        # z-subdomain lookup have it
        # (set later in _emit_signal)
        pass

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
            "volume_24h": float(vol or 0),
        })
    return out


def _confidence(hours_to_resolve: float, yes_price: float) -> float:
    """Higher conf for closer-to-resolution + lower entry price."""
    time_lift = 0.20 * (1 - hours_to_resolve / 24.0)
    price_lift = 0.15 * max(0.0, (0.20 - yes_price) / 0.10)
    return min(0.85, max(0.10, 0.50 + time_lift + price_lift))


def _emit_signal(market: dict, api_url: str) -> None:
    """Insert into market_signals + signal_outcomes via API endpoint, OR
    direct DB write as a fallback. Mimics whale_signal_engine emission
    so the existing paper executor picks it up unchanged."""
    conn = get_connection()
    try:
        conf = _confidence(market["hours_to_resolve"], market["yes_price"])
        now_ts = int(time.time())
        # market_signals row — same shape as whale signals
        try:
            conn.execute(
                """INSERT INTO market_signals
                     (signal_type, condition_id, fired_at, side, direction,
                      entry_price, confidence, wallet, wallet_tier,
                      slug, question, category)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    SIGNAL_TYPE, market["condition_id"], now_ts,
                    "YES", "BUY", market["yes_price"], conf,
                    "phase_b_resolution_decay", "synthetic",
                    market["slug"], market["question"][:200] or "",
                    None,
                ),
            )
            conn.commit()
        except Exception as exc:
            logger.debug("market_signals insert failed: %s", exc)

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
            "wallet": "phase_b_resolution_decay",
            "wallet_tier": "synthetic",
            "slug": market["slug"],
            "question": market["question"][:200],
            "yes_token_id": market["yes_token_id"],
            "no_token_id": market["no_token_id"],
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
