"""Edge survey #1 — the resolution→mark window.

QUESTION (2026-07-31): after the CTF oracle reports an outcome on-chain
(ConditionResolution), how long does the orderbook keep trading the
WINNING token below $1, and how much size is available there?

If the oracle has spoken, buying the winning side at $0.90 and redeeming
at $1.00 is deterministic profit — no forecast required. This is the only
structural edge the platform's own data has ever hinted at: the 2026-07-29
audit found resolution_decay firing on markets already resolved on-chain
13-15 minutes earlier, which means such a window exists at least sometimes.

This script measures it. Zero capital, read-only, pure history:

  * cohort  = conditions with a PRECISE chain_condition_resolution
              timestamp (block time; midnight values are date-derived and
              excluded) inside the firehose retention window
  * winner  = yes_token_id when resolves_yes else no_token_id
  * tape    = wallet_trades side='BUY' rows (one row per fill; the
              two-perspective recording is deduped by taking buyers only)

Reported per condition and in aggregate:
  * seconds from oracle report to the first print on the winning token
  * USD that traded on the winner BELOW each price threshold AFTER the
    report — the money left on the table
  * how long the sub-threshold window stayed open

FEASIBILITY NOTE: a window is only actionable if it outlives our reaction
time. The scheduler's fast lane is minutes; a sub-60s window is a market-
maker's game, not ours. The report bands the windows accordingly.

Run (ISOLATED — never exec heavy work into the live scheduler):
  docker compose run --rm --no-deps scheduler \
      python scripts/edge_survey_resolution_window.py [--days 5] [--limit 4000]
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

logger = logging.getLogger("edge_survey")

# Prices below this on a KNOWN winner are free money (minus the ~2-3c
# execution toll, which is why 0.95 is not the interesting threshold —
# 0.90 and below is where a real trade survives costs).
THRESHOLDS = (0.99, 0.95, 0.90, 0.80, 0.50)

# Reaction-time bands. Our fast lane is minutes, so anything that closes
# inside a minute is unreachable no matter how good the signal.
LATENCY_BANDS = ((0, 60, "<1min (unreachable)"),
                 (60, 300, "1-5min (tight)"),
                 (300, 900, "5-15min (reachable)"),
                 (900, 3600, "15-60min (comfortable)"),
                 (3600, 10**9, ">1h (wide open)"))


def _band(seconds: float) -> str:
    for lo, hi, label in LATENCY_BANDS:
        if lo <= seconds < hi:
            return label
    return "unknown"


def survey(days: int = 5, limit: int = 4000, db_path: str | None = None) -> dict:
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        now_row = conn.execute("SELECT EXTRACT(EPOCH FROM NOW())").fetchone()
        now = int(float(now_row[0]))
        floor_ts = now - days * 86400

        conds = conn.execute(
            """SELECT condition_id, resolved_at, resolves_yes,
                      yes_token_id, no_token_id
                 FROM market_resolutions
                WHERE source = 'chain_condition_resolution'
                  AND resolved_at IS NOT NULL
                  AND resolved_at >= ?
                  AND MOD(resolved_at, 86400) <> 0
                  AND resolves_yes IS NOT NULL
                  AND yes_token_id IS NOT NULL
                  AND no_token_id IS NOT NULL
                ORDER BY resolved_at DESC
                LIMIT ?""",
            (floor_ts, limit),
        ).fetchall()

        n_cond = len(conds)
        n_with_prints = 0
        first_print_lag: list[float] = []
        below: dict[float, dict] = {t: {"usd": 0.0, "n_conds": 0, "trades": 0}
                                    for t in THRESHOLDS}
        window_bands: dict[str, int] = {}
        per_cond_rows: list[dict] = []

        for cid, res_ts, res_yes, yes_tok, no_tok in conds:
            res_ts = int(res_ts)
            win_tok = str(yes_tok if int(res_yes) else no_tok)
            prints = conn.execute(
                """SELECT timestamp, price, size
                     FROM wallet_trades
                    WHERE asset = ? AND side = 'BUY' AND timestamp >= ?
                    ORDER BY timestamp
                    LIMIT 5000""",
                (win_tok, res_ts),
            ).fetchall()
            if not prints:
                continue
            n_with_prints += 1
            lag = float(prints[0][0]) - res_ts
            first_print_lag.append(lag)

            cheap_usd = 0.0
            last_cheap_ts = None
            for ts, px, sz in prints:
                px = float(px or 0)
                sz = float(sz or 0)
                usd = px * sz
                for t in THRESHOLDS:
                    if px <= t:
                        below[t]["usd"] += usd
                        below[t]["trades"] += 1
                if px <= 0.90:
                    cheap_usd += usd
                    last_cheap_ts = int(ts)
            # count conditions (not trades) offering each threshold
            seen_t = set()
            for ts, px, sz in prints:
                for t in THRESHOLDS:
                    if float(px or 0) <= t and t not in seen_t:
                        below[t]["n_conds"] += 1
                        seen_t.add(t)

            if last_cheap_ts is not None:
                window_s = last_cheap_ts - res_ts
                window_bands[_band(window_s)] = window_bands.get(_band(window_s), 0) + 1
                per_cond_rows.append({
                    "cid": cid[:14], "lag_s": round(lag),
                    "window_s": window_s, "cheap_usd": round(cheap_usd, 2),
                })

        per_cond_rows.sort(key=lambda r: -r["cheap_usd"])
        out = {
            "cohort_days": days,
            "conditions_resolved_on_chain": n_cond,
            "conditions_with_post_resolution_prints": n_with_prints,
            "first_print_lag_s": {
                "median": round(statistics.median(first_print_lag), 1)
                if first_print_lag else None,
                "p25": round(statistics.quantiles(first_print_lag, n=4)[0], 1)
                if len(first_print_lag) > 3 else None,
                "p75": round(statistics.quantiles(first_print_lag, n=4)[2], 1)
                if len(first_print_lag) > 3 else None,
            },
            "traded_below_threshold_after_resolution": {
                f"<= {t}": {"usd": round(v["usd"], 2),
                            "conditions": v["n_conds"],
                            "trades": v["trades"]}
                for t, v in below.items()
            },
            "sub_0.90_window_duration": window_bands,
            "top_opportunities": per_cond_rows[:15],
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
    path = PROJECT_ROOT / "reports" / f"edge_survey_resolution_window_{day}.md"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            f"# Edge survey — resolution→mark window ({day})",
            "",
            "After the CTF oracle reports an outcome on-chain, does the "
            "orderbook keep trading the WINNING token below $1 — and for "
            "how long, at what size? Buying a known winner below $1 and "
            "redeeming at $1 is deterministic profit.",
            "",
            f"- Cohort: {out['conditions_resolved_on_chain']} conditions with "
            f"precise on-chain resolution in the last {out['cohort_days']}d",
            f"- With any post-resolution print on the winner: "
            f"**{out['conditions_with_post_resolution_prints']}**",
            f"- First print after oracle report (s): "
            f"median {out['first_print_lag_s']['median']}, "
            f"p25 {out['first_print_lag_s']['p25']}, "
            f"p75 {out['first_print_lag_s']['p75']}",
            "",
            "## Money traded on a KNOWN winner, after the oracle spoke",
            "",
            "| price | USD traded | conditions | trades |",
            "|---|---|---|---|",
        ]
        for k, v in out["traded_below_threshold_after_resolution"].items():
            lines.append(f"| {k} | ${v['usd']:,.2f} | {v['conditions']} | "
                         f"{v['trades']} |")
        lines += ["", "## How long the sub-$0.90 window stayed open", "",
                  "| duration | conditions |", "|---|---|"]
        for k, v in sorted(out["sub_0.90_window_duration"].items(),
                           key=lambda kv: -kv[1]):
            lines.append(f"| {k} | {v} |")
        lines += ["", "## Largest single opportunities", "",
                  "| condition | lag(s) | window(s) | USD under $0.90 |",
                  "|---|---|---|---|"]
        for r in out["top_opportunities"]:
            lines.append(f"| {r['cid']} | {r['lag_s']} | {r['window_s']} | "
                         f"${r['cheap_usd']:,.2f} |")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        logger.info("[edge_survey] report → %s", path)
    except Exception as exc:
        logger.warning("[edge_survey] report write failed: %s", exc)


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=5)
    ap.add_argument("--limit", type=int, default=4000)
    args = ap.parse_args()
    print(json.dumps(survey(days=args.days, limit=args.limit), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
