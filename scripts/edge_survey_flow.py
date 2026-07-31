"""Edge survey #3 — does aggregate order flow predict price?

QUESTION (2026-07-31): per-wallet copy trading is dead (multiple
pre-registered kills: skill doesn't persist, winners regress). But
AGGREGATE order-flow imbalance is a different statistic entirely — it
asks whether the *net direction of pressure* across ALL participants
forecasts the next move, which is the most robust finding in equity and
crypto microstructure. We hold every fill on the exchange, so if the
effect exists here we can see it.

DATA SUBTLETY THAT SHAPES THE METHOD. `wallet_trades.side` does NOT mark
the aggressor: the firehose persists every OrderFilled from BOTH
perspectives, so each fill writes one BUY row (the maker who bought) and
one SELL row (the taker who sold), and aggregate BUY volume equals
aggregate SELL volume by construction (149,526 vs 144,673 over the same
62,944 transactions in a 1h sample). Raw buy-minus-sell is therefore
identically ~zero and useless.

So this uses the TICK RULE — the standard classifier when quote data is
unavailable: a print above the previous print is buyer-initiated, below
is seller-initiated, unchanged inherits the prior sign. It needs no
assumption about our recording at all.

METHOD (deliberately conservative, after the parity-v1 lesson):
  * universe = tokens with >= MIN_PRINTS fills in the window. This is
    also the LIQUIDITY FILTER the cost analysis demands: our ~2-3c round
    trip is ~20% of a $0.15 position, so illiquid longshots can never
    pay, however good the signal.
  * bucket the tape into BUCKET_S seconds; per bucket compute tick-rule
    signed volume / total volume = imbalance in [-1, +1], and VWAP.
  * signal at bucket i = imbalance summed over the trailing LOOKBACK
    buckets. Target = VWAP(i + HORIZON) - VWAP(i), in CENTS.
  * any forward window crossing the market's resolution is DROPPED —
    otherwise the 0/1 settlement jump swamps everything and manufactures
    a phantom effect.
  * results are reported in cents per share against the 2-3c toll, by
    signal quintile, with n. A signal that predicts 0.4c is worthless
    even at p<0.001.

Run (ISOLATED — never exec heavy work into the live scheduler):
  docker compose run --rm --no-deps scheduler \
      python scripts/edge_survey_flow.py [--days 4] [--horizon 5]
"""
from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from trading_platform.polymarket.db_connection import get_connection  # noqa: E402

logger = logging.getLogger("edge_survey_flow")

BUCKET_S = 60
LOOKBACK = 5          # buckets of flow history feeding the signal
MIN_PRINTS = 200      # liquidity floor per token
MAX_TOKENS = 150      # bound the survey's compute
TOLL_CENTS = 2.5      # round-trip execution cost we must beat


def _tick_sign(px: float, prev_px: float | None, prev_sign: int) -> int:
    if prev_px is None:
        return 0
    if px > prev_px:
        return 1
    if px < prev_px:
        return -1
    return prev_sign


def survey(days: int = 4, horizon: int = 5, db_path: str | None = None) -> dict:
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        now = int(float(conn.execute(
            "SELECT EXTRACT(EPOCH FROM NOW())").fetchone()[0]))
        floor_ts = now - days * 86400

        toks = conn.execute(
            """SELECT asset, COUNT(*) n
                 FROM wallet_trades
                WHERE timestamp >= ? AND side = 'BUY' AND asset IS NOT NULL
                GROUP BY asset
               HAVING COUNT(*) >= ?
                ORDER BY n DESC
                LIMIT ?""",
            (floor_ts, MIN_PRINTS, MAX_TOKENS),
        ).fetchall()

        # resolution cut-offs so no forward window spans settlement
        res_by_token: dict[str, int] = {}
        for tok, _ in toks:
            row = conn.execute(
                """SELECT MIN(resolved_at) FROM market_resolutions
                    WHERE resolved_at IS NOT NULL
                      AND (yes_token_id = ? OR no_token_id = ?)""",
                (str(tok), str(tok)),
            ).fetchone()
            if row and row[0]:
                res_by_token[str(tok)] = int(row[0])

        samples: list[tuple[float, float]] = []   # (signal, forward cents)
        tokens_used = 0
        dropped_resolution = 0

        for tok, _n in toks:
            prints = conn.execute(
                """SELECT timestamp, price, size
                     FROM wallet_trades
                    WHERE asset = ? AND side = 'BUY' AND timestamp >= ?
                    ORDER BY timestamp""",
                (str(tok), floor_ts),
            ).fetchall()
            if len(prints) < MIN_PRINTS:
                continue

            buckets: dict[int, dict] = {}
            prev_px = None
            prev_sign = 0
            for ts, px, sz in prints:
                px = float(px or 0)
                sz = float(sz or 0)
                if px <= 0 or sz <= 0:
                    continue
                sign = _tick_sign(px, prev_px, prev_sign)
                prev_px, prev_sign = px, sign
                b = int(ts) // BUCKET_S
                d = buckets.setdefault(b, {"signed": 0.0, "vol": 0.0,
                                           "pv": 0.0})
                d["signed"] += sign * sz
                d["vol"] += sz
                d["pv"] += px * sz

            keys = sorted(buckets)
            if len(keys) < LOOKBACK + horizon + 2:
                continue
            tokens_used += 1
            res_ts = res_by_token.get(str(tok))

            for i in range(LOOKBACK, len(keys) - horizon):
                b_now = keys[i]
                b_fwd = keys[i + horizon]
                # contiguity: forward bucket must be within horizon*2 of now,
                # else the gap makes the "return" meaningless
                if b_fwd - b_now > horizon * 2:
                    continue
                if res_ts is not None and b_fwd * BUCKET_S >= res_ts:
                    dropped_resolution += 1
                    continue
                sig = 0.0
                tot = 0.0
                for j in range(i - LOOKBACK, i):
                    d = buckets[keys[j]]
                    sig += d["signed"]
                    tot += d["vol"]
                if tot <= 0:
                    continue
                imbalance = sig / tot
                p_now = buckets[b_now]["pv"] / buckets[b_now]["vol"]
                p_fwd = buckets[b_fwd]["pv"] / buckets[b_fwd]["vol"]
                samples.append((imbalance, (p_fwd - p_now) * 100.0))

        if len(samples) < 100:
            return {"error": "insufficient samples", "n": len(samples),
                    "tokens_used": tokens_used}

        samples.sort(key=lambda s: s[0])
        n = len(samples)
        q = n // 5
        quintiles = []
        for k in range(5):
            lo = k * q
            hi = (k + 1) * q if k < 4 else n
            chunk = samples[lo:hi]
            rets = [c[1] for c in chunk]
            quintiles.append({
                "quintile": k + 1,
                "imbalance_range": [round(chunk[0][0], 3),
                                    round(chunk[-1][0], 3)],
                "n": len(chunk),
                "mean_fwd_cents": round(statistics.mean(rets), 4),
                "median_fwd_cents": round(statistics.median(rets), 4),
                "stdev_cents": round(statistics.pstdev(rets), 3),
            })

        top = quintiles[-1]["mean_fwd_cents"]
        bot = quintiles[0]["mean_fwd_cents"]
        spread = top - bot
        out = {
            "cohort_days": days,
            "bucket_seconds": BUCKET_S,
            "lookback_buckets": LOOKBACK,
            "horizon_buckets": horizon,
            "tokens_used": tokens_used,
            "samples": n,
            "dropped_crossing_resolution": dropped_resolution,
            "quintiles": quintiles,
            "long_short_spread_cents": round(spread, 4),
            "toll_cents": TOLL_CENTS,
            "verdict": ("TRADEABLE" if spread > TOLL_CENTS
                        else "below execution toll — not tradeable"),
        }
        _write_report(out)
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _write_report(out: dict) -> None:
    day = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    path = PROJECT_ROOT / "reports" / f"edge_survey_flow_{day}.md"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            f"# Edge survey — aggregate order flow ({day})",
            "",
            "Does tick-rule order-flow imbalance predict the next move? "
            "`side` cannot be used (the firehose records both maker and "
            "taker perspectives, so buy volume == sell volume by "
            "construction), hence the tick rule.",
            "",
            f"- Tokens (>= {MIN_PRINTS} prints, {out['cohort_days']}d): "
            f"{out['tokens_used']}",
            f"- Samples: {out['samples']:,} "
            f"({out['bucket_seconds']}s buckets, {out['lookback_buckets']} "
            f"back, {out['horizon_buckets']} forward)",
            f"- Dropped for crossing resolution: "
            f"{out['dropped_crossing_resolution']:,}",
            "",
            "## Forward return by flow-imbalance quintile",
            "",
            "| quintile | imbalance | n | mean fwd (¢) | median (¢) | stdev (¢) |",
            "|---|---|---|---|---|---|",
        ]
        for q in out["quintiles"]:
            lines.append(
                f"| Q{q['quintile']} | {q['imbalance_range'][0]}.."
                f"{q['imbalance_range'][1]} | {q['n']:,} | "
                f"{q['mean_fwd_cents']:+.4f} | {q['median_fwd_cents']:+.4f} | "
                f"{q['stdev_cents']:.3f} |")
        lines += [
            "",
            f"**Q5 − Q1 spread: {out['long_short_spread_cents']:+.4f}¢** "
            f"vs a {out['toll_cents']}¢ round-trip toll → "
            f"**{out['verdict']}**",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        logger.info("[flow] report → %s", path)
    except Exception as exc:
        logger.warning("[flow] report write failed: %s", exc)


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=4)
    ap.add_argument("--horizon", type=int, default=5)
    args = ap.parse_args()
    print(json.dumps(survey(days=args.days, horizon=args.horizon), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
