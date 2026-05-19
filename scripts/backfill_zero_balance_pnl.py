"""Backfill realized_pnl on live_trades rows closed via resolved_zero_balance.

Pre-fix behavior: the close path always booked realized_pnl = -size_usd and
outcome = 'loss'. This overstates losses when:
  (a) fill_price IS NULL or shares <= 0 — order never opened, P&L should be 0
  (b) shares > 0 but actual cost basis = shares * (1 - fill_price) [SELL] or
      shares * fill_price [BUY] is far below size_usd

Run with --dry-run first to inspect, then without to apply.

Updates rows where exit_reason='resolved_zero_balance' AND dry_run=0.
"""
import sys, os, time, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
from trading_platform.polymarket.db_connection import get_connection


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Actually update rows. Default is dry-run.")
    args = ap.parse_args()

    conn = get_connection()
    rows = conn.execute("""
        SELECT id, direction, fill_price, shares, size_usd, realized_pnl, outcome, status
          FROM live_trades
         WHERE dry_run=0 AND exit_reason='resolved_zero_balance'
    """).fetchall()

    print(f"Found {len(rows)} rows with exit_reason='resolved_zero_balance'")
    print(f"{'id':>5} {'st':<8} {'dir':<5} {'fill':>7} {'shares':>7} {'size':>6} "
          f"{'pnl_was':>8} {'pnl_new':>9} {'outc_was':<8} {'outc_new':<8}")

    updates = []
    delta_total = 0.0
    for r in rows:
        rid, direction, fp, sh, sz, pnl_was, outc_was, status = r
        fp_v = float(fp) if fp is not None else None
        sh_v = float(sh or 0)
        pnl_was = float(pnl_was or 0)

        # Three cases for resolved_zero_balance:
        #   1. status != 'matched' (e.g. 'live', 'cancelled'): order never filled,
        #      no capital was committed on-chain → P&L = $0
        #   2. status='matched' but fp/shares missing: same — never opened
        #   3. status='matched' with fp + shares: legitimate fill that later went
        #      to zero balance (resolved against us) → P&L = -(cost basis)
        if status != "matched":
            pnl_new = 0.0
            outc_new = None  # never matched on-chain
        elif fp_v is None or sh_v <= 0:
            pnl_new = 0.0
            outc_new = None
        else:
            if (direction or "").upper() == "BUY":
                pnl_new = -sh_v * fp_v
            else:
                pnl_new = -sh_v * (1.0 - fp_v)
            outc_new = "loss" if pnl_new < 0 else None

        pnl_new = round(pnl_new, 4)
        if abs(pnl_new - pnl_was) < 0.001 and outc_new == outc_was:
            continue  # no change needed

        delta_total += (pnl_new - pnl_was)
        print(f"{rid:>5} {str(status or ''):<8} {str(direction):<5} "
              f"{('N/A' if fp_v is None else f'{fp_v:.3f}'):>7} "
              f"{sh_v:>7.2f} {float(sz or 0):>6.2f} "
              f"{pnl_was:>+8.2f} {pnl_new:>+9.4f} "
              f"{str(outc_was):<8} {str(outc_new):<8}")
        updates.append((pnl_new, outc_new, rid))

    print(f"\nRows changed: {len(updates)}")
    print(f"Total realized_pnl delta: ${delta_total:+.4f}")
    print(f"  (cumulative all-time realized_pnl will shift by ${delta_total:+.2f})")

    if not args.apply:
        print("\n[dry-run] No changes written. Re-run with --apply to commit.")
        conn.close()
        return 0

    for pnl_new, outc_new, rid in updates:
        conn.execute(
            "UPDATE live_trades SET realized_pnl=%s, outcome=%s WHERE id=%s",
            (pnl_new, outc_new, rid),
        )
    conn.commit()
    conn.close()
    print(f"\nApplied {len(updates)} updates.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
