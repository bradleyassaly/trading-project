"""Stage-B readiness check for borrow #2 (VWAP-slippage gate).

Answers: "do we have enough live resolution_decay data yet to decide whether
to gate on vwap_slippage_c?" Reports three things, all read-only:

  1. DATA READINESS — how many live fills carry both the predicted
     (vwap_slippage_c) and realized (slippage_c) cost, how many have RESOLVED,
     and how many DISTINCT events they span (fills on one game are not
     independent samples — G3 event-clustering).
  2. CALIBRATION — does the book-walk prediction track the realized fill cost?
     (mean predicted vs realized, correlation). If it doesn't predict, the gate
     has nothing to stand on.
  3. OUTCOME SPLIT — do high-predicted-slippage fills actually resolve worse
     net-of-cost? Buckets resolved fills at the median vwap_slippage_c and
     compares realized PnL / win rate. This is the gate's justification.

Then a VERDICT against conservative thresholds. Run it weekly:
    python -m scripts.measure_vwap_slippage_stageb      (or: python scripts/…)

Nothing here changes state. The gate itself (Stage B) stays unbuilt until this
says READY on held-out, event-clustered, net-of-cost evidence.
"""
from __future__ import annotations

import sys

# Conservative promotion thresholds (event-clustered).
MIN_FILLS_CALIB = 30      # fills with both predicted+realized to judge calibration
MIN_RESOLVED = 30         # resolved fills to judge outcome split
MIN_EVENTS = 20           # distinct condition_ids among resolved (independence)


def _col_exists(conn, table: str, col: str) -> bool:
    try:
        cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        return col in cols
    except Exception:
        return False


def _pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    num = sum((a - mx) * (b - my) for a, b in zip(xs, ys))
    dx = sum((a - mx) ** 2 for a in xs) ** 0.5
    dy = sum((b - my) ** 2 for b in ys) ** 0.5
    if dx == 0 or dy == 0:
        return None
    return num / (dx * dy)


def main() -> int:
    from trading_platform.polymarket.db_connection import get_connection
    conn = get_connection()
    print("=" * 66)
    print("STAGE-B READINESS — #2 VWAP-slippage gate (resolution_decay, LIVE)")
    print("=" * 66)

    if not _col_exists(conn, "live_trades", "vwap_slippage_c"):
        print("\n[NOT DEPLOYED] live_trades has no vwap_slippage_c column yet.")
        print("The instrumentation ships in the resolution-engine-hardening")
        print("branch; the idempotent migration adds the column when the")
        print("updated live executor first runs. Deploy the branch, then the")
        print("column populates on every live attempt reaching the depth guard.")
        return 0

    # ── 1. Data readiness ──────────────────────────────────────────────────
    # Real live attempts (exclude dry-run + execution probes) that reached the
    # guard (vwap_slippage_c populated).
    base = ("FROM live_trades WHERE signal_type = 'resolution_decay' "
            "AND COALESCE(dry_run,0) = 0 AND COALESCE(is_probe,0) = 0 "
            "AND vwap_slippage_c IS NOT NULL")
    n_instr = conn.execute(f"SELECT COUNT(*) {base}").fetchone()[0]
    n_fills = conn.execute(
        f"SELECT COUNT(*) {base} AND fill_price IS NOT NULL "
        f"AND slippage_c IS NOT NULL").fetchone()[0]
    resolved = conn.execute(
        f"SELECT vwap_slippage_c, slippage_c, realized_pnl, size_usd, "
        f"       condition_id, outcome "
        f"{base} AND fill_price IS NOT NULL AND realized_pnl IS NOT NULL"
    ).fetchall()
    n_resolved = len(resolved)
    n_events = len({r[4] for r in resolved if r[4]})
    rng = conn.execute(
        f"SELECT MIN(attempted_at), MAX(attempted_at) {base} "
        f"AND fill_price IS NOT NULL").fetchone()
    span_days = ((rng[1] - rng[0]) / 86400) if rng[0] and rng[1] else 0

    # Distinguish "column exists but old code is running" from "genuinely no
    # activity". If resolution_decay is firing but nothing populates the
    # column, the deployed image predates the instrumentation.
    if n_instr == 0:
        try:
            recent = conn.execute(
                "SELECT COUNT(*) FROM live_trades WHERE signal_type = "
                "'resolution_decay' AND attempted_at > "
                "EXTRACT(EPOCH FROM NOW())::BIGINT - 86400").fetchone()[0]
        except Exception:
            recent = 0
        if recent > 0:
            print(f"\n[NOT DEPLOYED] {recent} resolution_decay attempts in the last")
            print("24h, but 0 populate vwap_slippage_c — the running containers are")
            print("on an image that predates this instrumentation. Deploy the")
            print("resolution-engine-hardening branch (rebuild the executor image)")
            print("for data to start flowing. Merging to main is not enough on its")
            print("own unless a deploy follows.")
            return 0

    print(f"\n1. DATA READINESS")
    print(f"   instrumented live attempts (vwap_slippage_c set): {n_instr}")
    print(f"   live FILLS with predicted+realized cost:          {n_fills}")
    print(f"   of those, RESOLVED (realized_pnl booked):         {n_resolved}")
    print(f"   distinct events among resolved:                   {n_events}")
    if n_fills and span_days > 0:
        rate = n_fills / span_days * 7
        print(f"   fill span: {span_days:.1f} days  (~{rate:.1f} fills/week)")
        if n_resolved < MIN_RESOLVED and rate > 0:
            weeks = (MIN_RESOLVED - n_resolved) / max(rate, 1e-9)
            print(f"   → at this rate, ~{weeks:.0f} more weeks to {MIN_RESOLVED} resolved")

    # ── 2. Calibration (predicted vs realized) ─────────────────────────────
    fills = conn.execute(
        f"SELECT vwap_slippage_c, slippage_c {base} "
        f"AND fill_price IS NOT NULL AND slippage_c IS NOT NULL").fetchall()
    print(f"\n2. CALIBRATION (does book-walk predict realized fill cost?)")
    if len(fills) >= 3:
        pred = [float(a) for a, b in fills]
        real = [float(b) for a, b in fills]
        r = _pearson(pred, real)
        print(f"   n={len(fills)}  mean predicted={sum(pred)/len(pred):.2f}c  "
              f"mean realized={sum(real)/len(real):.2f}c")
        print(f"   pearson(pred, realized) = "
              f"{('%.3f' % r) if r is not None else 'n/a'}")
        me = sum(abs(a - b) for a, b in zip(pred, real)) / len(fills)
        print(f"   mean abs error = {me:.2f}c "
              f"(lower = book-walk tracks the fill better)")
    else:
        print(f"   n={len(fills)} — need >= {MIN_FILLS_CALIB} to judge")

    # ── 3. Outcome split (high vs low predicted slippage) ──────────────────
    print(f"\n3. OUTCOME SPLIT (do high-predicted-slippage fills resolve worse?)")
    if n_resolved >= 4:
        preds = sorted(float(r[0]) for r in resolved)
        med = preds[len(preds) // 2]

        def _bucket(hi: bool):
            rows = [r for r in resolved
                    if (float(r[0]) >= med) == hi]
            if not rows:
                return None
            # net-of-cost return per $ staked
            rets = [float(r[2]) / float(r[3]) for r in rows
                    if r[3] and float(r[3]) > 0]
            wins = sum(1 for r in rows if (r[5] or "").lower().startswith("win")
                       or (r[2] is not None and float(r[2]) > 0))
            avg_ret = sum(rets) / len(rets) if rets else float("nan")
            return len(rows), avg_ret, wins / len(rows)

        lo = _bucket(False)
        hi = _bucket(True)
        print(f"   split at median predicted slippage = {med:.2f}c")
        if lo:
            print(f"   LOW  slip: n={lo[0]:<3} avg net return/$ ={lo[1]:+.3f}  "
                  f"win%={lo[2]*100:.0f}")
        if hi:
            print(f"   HIGH slip: n={hi[0]:<3} avg net return/$ ={hi[1]:+.3f}  "
                  f"win%={hi[2]*100:.0f}")
        if lo and hi:
            edge = lo[1] - hi[1]
            print(f"   gate signal = LOW − HIGH return/$ = {edge:+.3f} "
                  f"(positive ⇒ high slippage really does cost you)")
    else:
        print(f"   n={n_resolved} resolved — need >= {MIN_RESOLVED}")

    # ── Verdict ────────────────────────────────────────────────────────────
    print(f"\nVERDICT")
    ready = (n_fills >= MIN_FILLS_CALIB and n_resolved >= MIN_RESOLVED
             and n_events >= MIN_EVENTS)
    if ready:
        print("   READY to evaluate a Stage-B gate. Take the outcome split to a")
        print("   held-out, event-clustered, net-of-cost test; if HIGH-slippage")
        print("   fills underperform beyond noise, ship a flag-gated skip/shrink")
        print("   on a shadow ladder first.")
    else:
        need = []
        if n_fills < MIN_FILLS_CALIB:
            need.append(f"{MIN_FILLS_CALIB - n_fills} more fills")
        if n_resolved < MIN_RESOLVED:
            need.append(f"{MIN_RESOLVED - n_resolved} more resolved")
        if n_events < MIN_EVENTS:
            need.append(f"{MIN_EVENTS - n_events} more distinct events")
        print(f"   NOT YET — need: {', '.join(need) if need else 'more data'}.")
        print("   Keep accumulating; nothing gates on vwap_slippage_c until this")
        print("   clears the bar.")
    try: conn.close()
    except Exception: pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
