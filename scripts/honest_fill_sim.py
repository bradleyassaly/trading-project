"""Honest fill simulator — re-test paper signals against the ACTUAL tape.

The paper engine's P&L is fantasy-priced (2026-07-16 ALPHA VERDICT: TP
exits at transient mid-spikes on illiquid books, 78% unfillable; same-
signal 30d 2026-07-27: paper +$7.33/trade vs live −$0.48/trade). This
script replays closed paper trades against the firehose tape
(wallet_trades = every OrderFilled on the exchange, both perspectives)
and asks two questions the paper engine never did:

  1. ENTRY: did anyone actually print at ≤ our claimed entry price on our
     token within ENTRY_WINDOW_S after the signal? (cum printed size must
     cover our share count — a 1-share dust print doesn't fill a $5 order)
  2. TP EXIT: for take_profit claims, did anyone print at ≥ our claimed
     exit price between entry and resolution? If NOT, the trade RIDES to
     resolution and gets the resolution payout instead of the TP fantasy.

Verdict per signal_type: paper EV/$ vs honest EV/$ and the fill rates.
Promotion decisions read THIS table, not the raw paper P&L.

INTERPRETATION (learned on the first run, 2026-07-28): the entry check is
a RESTING-fillability standard — "would a passive order at the claimed
price have been hit by real flow". It is the right standard for
maker-style execution and for TP exits (a resting sell IS the mechanism),
but it is STRICTER than the paper engine's implicit taker fill (crossing
the book fills even when nobody else trades). First-run finding:
resolution_decay paper entries are 0/42 resting-fillable — 8/10 sampled
tokens printed NOTHING for a full hour after the signal. That is the
signal's own thesis (flow has dried up) measured from the tape, and it
kills maker-style execution (E1) for decay outright. A taker-grade check
needs book state at paper fire time, which paper does not capture yet
(order_book_snapshots covers only 2/42 of these markets) — see the
paper-book-capture chip before trusting entry_fill for taker signals.

Assumptions (all conservative-stated, v1):
  * a print at ≤ L after our signal ⇒ our resting bid at L would have
    filled (queue position ignored — mildly optimistic; offset by
    requiring FULL size coverage, which is pessimistic on partials)
  * stop/trailing/expired/resolved exits keep the paper engine's booked
    P&L (stops fill WORSE live than paper — so honest EV is an UPPER
    bound for stop-heavy signals; noted per row)
  * only YES-side paper trades (the engine writes side='YES' only)
  * tape completeness: the DB holds the FULL market tape for the last ~7
    days only (firehose retention prunes non-roster rows; roster rows
    would bias fills sparse-downward) — so the covered cohort is
    entry_ts ≥ now − TAPE_SAFE_DAYS. Re-run weekly; n accrues. The
    2026-07-20+ parquet archive can extend this later.

Read-only vs the DB except the standing verdict table
honest_fill_verdicts (full refresh per run, one row per signal_type).

Run (ISOLATED — never exec into the live scheduler):
  docker compose run --rm --no-deps scheduler \
      python scripts/honest_fill_sim.py [--days 6] [--window-s 300]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from trading_platform.polymarket.db_connection import get_connection  # noqa: E402

logger = logging.getLogger("honest_fill_sim")

TAPE_SAFE_DAYS = 6          # inside the 7d non-roster retention window
ENTRY_WINDOW_S = 300        # resting-entry patience after signal
TP_REASONS = ("take_profit",)

_VERDICT_SCHEMA = """
CREATE TABLE IF NOT EXISTS honest_fill_verdicts (
    signal_type      TEXT PRIMARY KEY,
    window_days      BIGINT,
    n_paper          BIGINT,
    n_entry_filled   BIGINT,
    n_tp_claims      BIGINT,
    n_tp_honest      BIGINT,
    n_unresolved     BIGINT,
    paper_pnl        DOUBLE PRECISION,
    honest_pnl       DOUBLE PRECISION,
    paper_ev_per_usd DOUBLE PRECISION,
    honest_ev_per_usd DOUBLE PRECISION,
    computed_at      BIGINT NOT NULL
);
"""


def _prints(conn, token_id: str, t0: int, t1: int, max_price: float | None,
            min_price: float | None) -> list[tuple[int, float, float]]:
    """Actual prints (ts, price, size) for a token in [t0, t1], optionally
    price-filtered. side='BUY' rows only — every fill has exactly one buyer
    row, so this dedupes the two-perspective recording."""
    conds = ["asset = ?", "side = 'BUY'", "timestamp >= ?", "timestamp <= ?"]
    args: list = [token_id, t0, t1]
    if max_price is not None:
        conds.append("price <= ?")
        args.append(max_price)
    if min_price is not None:
        conds.append("price >= ?")
        args.append(min_price)
    rows = conn.execute(
        f"SELECT timestamp, price, size FROM wallet_trades "
        f"WHERE {' AND '.join(conds)} ORDER BY timestamp",
        tuple(args),
    ).fetchall()
    return [(int(r[0]), float(r[1]), float(r[2])) for r in rows]


def run_sim(days: int = TAPE_SAFE_DAYS, entry_window_s: int = ENTRY_WINDOW_S,
            db_path: str | None = None) -> dict:
    now = int(time.time())
    floor_ts = now - days * 86400
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        trades = conn.execute(
            """SELECT pt.id, pt.signal_type, pt.condition_id,
                      COALESCE(pt.raw_entry_price, pt.entry_price),
                      COALESCE(pt.raw_exit_price, pt.exit_price),
                      pt.size_usd, pt.realized_pnl, pt.exit_reason,
                      pt.entry_ts, pt.exit_ts,
                      m.yes_token_id,
                      mr.resolves_yes, mr.payout_yes
                 FROM polymarket_paper_trades pt
                 LEFT JOIN markets m ON m.condition_id = pt.condition_id
                 LEFT JOIN market_resolutions mr
                        ON mr.condition_id = pt.condition_id
                WHERE pt.entry_ts >= ?
                  AND pt.exit_ts IS NOT NULL
                  AND pt.realized_pnl IS NOT NULL
                  AND COALESCE(pt.side, 'YES') = 'YES'""",
            (floor_ts,),
        ).fetchall()

        per_sig: dict[str, dict] = {}
        for (tid, sig, cid, entry_px, exit_px, size_usd, paper_pnl, reason,
             entry_ts, exit_ts, yes_tok, resolves_yes, payout_yes) in trades:
            sig = sig or "unknown"
            s = per_sig.setdefault(sig, {
                "n_paper": 0, "n_entry_filled": 0, "n_tp_claims": 0,
                "n_tp_honest": 0, "n_unresolved": 0, "paper_pnl": 0.0,
                "honest_pnl": 0.0, "paper_staked": 0.0, "honest_staked": 0.0,
            })
            entry_px = float(entry_px or 0)
            size_usd = float(size_usd or 0)
            paper_pnl_f = float(paper_pnl or 0)
            s["n_paper"] += 1
            s["paper_pnl"] += paper_pnl_f
            s["paper_staked"] += size_usd
            if not yes_tok or entry_px <= 0 or size_usd <= 0:
                s["n_unresolved"] += 1
                continue
            shares = size_usd / entry_px

            # 1. ENTRY: cum printed size at ≤ entry_px within the window.
            fills = _prints(conn, str(yes_tok), int(entry_ts),
                            int(entry_ts) + entry_window_s, entry_px, None)
            if sum(f[2] for f in fills) < shares:
                continue  # honest verdict: never entered — trade drops
            s["n_entry_filled"] += 1
            s["honest_staked"] += size_usd

            # 2. EXIT
            if (reason or "") in TP_REASONS:
                s["n_tp_claims"] += 1
                exit_px_f = float(exit_px or 0)
                tp_fills = _prints(conn, str(yes_tok), int(entry_ts), now,
                                   None, exit_px_f) if exit_px_f > 0 else []
                if exit_px_f > 0 and sum(f[2] for f in tp_fills) >= shares:
                    s["n_tp_honest"] += 1
                    s["honest_pnl"] += (exit_px_f - entry_px) * shares
                else:
                    # TP never printed — the trade RIDES to resolution.
                    if payout_yes is not None:
                        pps = float(payout_yes)
                    elif resolves_yes is not None:
                        pps = 1.0 if int(resolves_yes) else 0.0
                    else:
                        s["n_unresolved"] += 1
                        s["honest_staked"] -= size_usd
                        s["n_entry_filled"] -= 1
                        continue
                    s["honest_pnl"] += (pps - entry_px) * shares
            else:
                # stop/trailing/expired/resolved: keep the paper booking
                # (stops fill WORSE live — honest EV is an upper bound).
                s["honest_pnl"] += paper_pnl_f

        # Verdict table (full refresh) + report.
        for stmt in _VERDICT_SCHEMA.strip().split(";"):
            if stmt.strip():
                try:
                    conn.execute(stmt)
                except Exception:
                    pass
        conn.execute("DELETE FROM honest_fill_verdicts")
        out_rows = []
        for sig, s in sorted(per_sig.items(),
                             key=lambda kv: -kv[1]["paper_pnl"]):
            p_ev = s["paper_pnl"] / s["paper_staked"] if s["paper_staked"] else 0.0
            h_ev = s["honest_pnl"] / s["honest_staked"] if s["honest_staked"] else 0.0
            conn.execute(
                """INSERT INTO honest_fill_verdicts
                     (signal_type, window_days, n_paper, n_entry_filled,
                      n_tp_claims, n_tp_honest, n_unresolved, paper_pnl,
                      honest_pnl, paper_ev_per_usd, honest_ev_per_usd,
                      computed_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT (signal_type) DO UPDATE SET
                     window_days = EXCLUDED.window_days,
                     n_paper = EXCLUDED.n_paper,
                     n_entry_filled = EXCLUDED.n_entry_filled,
                     n_tp_claims = EXCLUDED.n_tp_claims,
                     n_tp_honest = EXCLUDED.n_tp_honest,
                     n_unresolved = EXCLUDED.n_unresolved,
                     paper_pnl = EXCLUDED.paper_pnl,
                     honest_pnl = EXCLUDED.honest_pnl,
                     paper_ev_per_usd = EXCLUDED.paper_ev_per_usd,
                     honest_ev_per_usd = EXCLUDED.honest_ev_per_usd,
                     computed_at = EXCLUDED.computed_at""",
                (sig, days, s["n_paper"], s["n_entry_filled"],
                 s["n_tp_claims"], s["n_tp_honest"], s["n_unresolved"],
                 round(s["paper_pnl"], 2), round(s["honest_pnl"], 2),
                 round(p_ev, 4), round(h_ev, 4), now),
            )
            out_rows.append({
                "signal_type": sig, "n_paper": s["n_paper"],
                "entry_fill": f"{s['n_entry_filled']}/{s['n_paper']}",
                "tp_honest": f"{s['n_tp_honest']}/{s['n_tp_claims']}",
                "unresolved": s["n_unresolved"],
                "paper_pnl": round(s["paper_pnl"], 2),
                "honest_pnl": round(s["honest_pnl"], 2),
                "paper_ev_usd": round(p_ev, 3),
                "honest_ev_usd": round(h_ev, 3),
            })
        conn.commit()

        report = {
            "window_days": days, "entry_window_s": entry_window_s,
            "cohort_floor": datetime.fromtimestamp(
                floor_ts, tz=timezone.utc).isoformat(),
            "n_trades_covered": len(trades),
            "verdicts": out_rows,
        }
        _write_report(report)
        return report
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _write_report(report: dict) -> None:
    day = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    path = PROJECT_ROOT / "reports" / f"honest_fill_sim_{day}.md"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            f"# Honest fill simulation — {day}",
            "",
            f"Cohort: closed YES-side paper trades since {report['cohort_floor']}"
            f" ({report['window_days']}d tape-complete window),"
            f" entry patience {report['entry_window_s']}s.",
            "",
            "Entry honest ⇔ the tape printed ≤ our claimed entry price at our"
            " size within the window. TP honest ⇔ the tape printed ≥ the"
            " claimed TP price before resolution; otherwise the trade rides"
            " to the resolution payout. Stops/expiries keep paper booking"
            " (honest EV is an UPPER bound for stop-heavy signals).",
            "",
            "| signal | n | entry fill | TP honest | unresolved |"
            " paper P&L | honest P&L | paper EV/$ | honest EV/$ |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for v in report["verdicts"]:
            lines.append(
                f"| {v['signal_type']} | {v['n_paper']} | {v['entry_fill']} |"
                f" {v['tp_honest']} | {v['unresolved']} |"
                f" ${v['paper_pnl']:+.2f} | ${v['honest_pnl']:+.2f} |"
                f" {v['paper_ev_usd']:+.3f} | {v['honest_ev_usd']:+.3f} |")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        logger.info("[honest_fill] report → %s", path)
    except Exception as exc:
        logger.warning("[honest_fill] report write failed: %s", exc)


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=TAPE_SAFE_DAYS)
    ap.add_argument("--window-s", type=int, default=ENTRY_WINDOW_S)
    args = ap.parse_args()
    report = run_sim(days=args.days, entry_window_s=args.window_s)
    print(json.dumps(report, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
