"""Per-wallet revenue attribution and auto-tier feedback.

For every closed live_trade with a known signal_wallet, attribute the
realized PnL back to that wallet. Roll up over 30d (and 7d) into:
  - n_trades, wins, losses
  - total realized PnL
  - avg PnL/trade
  - Sharpe-like ratio (mean / stddev) when n>=5

Auto-tier downgrade: if a wallet's 30d realized PnL < AUTO_DEMOTE_THRESHOLD
on n >= AUTO_DEMOTE_N trades, the script flags it (default: print only;
pass --apply to actually update wallet tier).

Recovery (2026-07-16): demotion used to be a one-way ratchet — nothing
ever cleared a DEMOTED row, so the 2026-07-13 auto-demote of the
phase_b_resolution_decay pseudo-wallet silently blocked ALL live trading
for 3 days. --apply now also runs the recovery pass (see
plan_recoveries): DEMOTED wallets that stop meeting the demote criteria
flip to PROBATION (executor caps their stake at
WALLET_PROBATION_STAKE_USD, default $5), and PROBATION rows clear
entirely once the 30d window shows pnl >= PROBATION_CLEAR_PNL on
n >= AUTO_DEMOTE_N. Every transition — and especially a NEW demote of a
signal-slice pseudo-wallet, which is a de-facto strategy kill — is
surfaced with a delivered Telegram alert (send_governance_alerts).

Why: we record signal_wallet on every live trade but never roll up
"wallet X contributed $Y in 30d". Likely 20-30% of signals come from
unmeasured wallets (no live evidence either way). Without attribution
we can't auto-promote/demote — we're flying on inherited leaderboard
tiers from historical paper data.

Usage:
    python scripts/wallet_attribution.py          # report only
    python scripts/wallet_attribution.py --apply  # commit tier changes
    python scripts/wallet_attribution.py --json   # machine output
"""
from __future__ import annotations

import argparse
import html
import json
import math
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from trading_platform.polymarket.db_connection import get_connection


AUTO_DEMOTE_PNL = -10.0     # Demote wallets with 30d realized < -$10
AUTO_DEMOTE_N = 5            # ... if they have at least 5 resolved trades
# A PROBATION override clears entirely at breakeven-or-better 30d pnl on
# n >= AUTO_DEMOTE_N. Deliberately stricter than "not a demote candidate":
# a slice sitting at -$9 on n=6 stays capped at probation stake rather
# than silently returning to full size.
PROBATION_CLEAR_PNL = 0.0
TOP_N = 10                   # How many wallets to show in top/bottom


def gather_attribution(conn, now_ts: int, window_days: int) -> list[dict]:
    """Roll up realized PnL by signal_wallet over `window_days`."""
    cutoff = now_ts - window_days * 86400
    rows = conn.execute("""
        SELECT signal_wallet,
               COUNT(*) n,
               SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
               SUM(CASE WHEN outcome='loss' THEN 1 ELSE 0 END) losses,
               ROUND(SUM(COALESCE(realized_pnl, 0))::numeric, 2) pnl,
               ROUND(AVG(COALESCE(realized_pnl, 0))::numeric, 3) avg_pnl,
               ROUND(STDDEV_SAMP(COALESCE(realized_pnl, 0))::numeric, 3) sd_pnl
          FROM live_trades
         WHERE dry_run=0 AND exit_ts >= %s AND realized_pnl IS NOT NULL
           AND signal_wallet IS NOT NULL AND signal_wallet != ''
         GROUP BY signal_wallet
        HAVING COUNT(*) >= 1
         ORDER BY pnl DESC
    """, (cutoff,)).fetchall()
    out = []
    for r in rows:
        wallet, n, wins, losses, pnl, avg, sd = r
        n = int(n or 0)
        # Sharpe-ish: avg / sd when sd > 0 and n >= 5
        sharpe = None
        if n >= 5 and sd is not None and float(sd) > 0:
            sharpe = round(float(avg) / float(sd) * math.sqrt(n), 2)
        out.append({
            "wallet": wallet,
            "n": n,
            "wins": int(wins or 0),
            "losses": int(losses or 0),
            "pnl": float(pnl or 0),
            "avg": float(avg or 0),
            "sd": float(sd) if sd is not None else None,
            "sharpe": sharpe,
            "wr": (int(wins or 0) / n) if n else 0,
        })
    return out


def print_human(attr_30d: list[dict], attr_7d: list[dict]) -> None:
    print("\n" + "=" * 72)
    print("  PER-WALLET ATTRIBUTION — 30d realized PnL")
    print("=" * 72)
    if not attr_30d:
        print("  No wallets with attributable trades.")
        return

    total_pnl = sum(w["pnl"] for w in attr_30d)
    print(f"  Total tracked wallets: {len(attr_30d)}, sum PnL: ${total_pnl:+.2f}\n")

    print(f"  TOP {TOP_N} (drove revenue):")
    print(f"  {'wallet':<44s} {'n':>4} {'WR':>4} {'pnl':>9} {'avg':>7} {'sharpe':>7}")
    for w in attr_30d[:TOP_N]:
        sh = f"{w['sharpe']:.2f}" if w["sharpe"] is not None else "  -  "
        print(f"  {w['wallet'][:42]:<44s} {w['n']:>4d} {w['wr']*100:>3.0f}% "
              f"${w['pnl']:>+7.2f} ${w['avg']:>+5.2f}  {sh}")

    if len(attr_30d) > TOP_N:
        print(f"\n  BOTTOM {TOP_N} (cost revenue):")
        print(f"  {'wallet':<44s} {'n':>4} {'WR':>4} {'pnl':>9} {'avg':>7} {'sharpe':>7}")
        for w in attr_30d[-TOP_N:]:
            sh = f"{w['sharpe']:.2f}" if w["sharpe"] is not None else "  -  "
            print(f"  {w['wallet'][:42]:<44s} {w['n']:>4d} {w['wr']*100:>3.0f}% "
                  f"${w['pnl']:>+7.2f} ${w['avg']:>+5.2f}  {sh}")

    # Auto-demote candidates
    candidates = [w for w in attr_30d
                  if w["pnl"] <= AUTO_DEMOTE_PNL and w["n"] >= AUTO_DEMOTE_N]
    if candidates:
        print(f"\n  AUTO-DEMOTE candidates ({len(candidates)}): "
              f"30d PnL <= ${AUTO_DEMOTE_PNL} AND n >= {AUTO_DEMOTE_N}")
        for w in candidates:
            print(f"    {w['wallet'][:50]} n={w['n']} pnl=${w['pnl']:+.2f}")
    else:
        print("\n  No auto-demote candidates.")

    # 7d trend (anyone trending bad?)
    print(f"\n  7d snapshot (last week's drivers):")
    for w in attr_7d[:5]:
        print(f"    {w['wallet'][:44]:<46s} n={w['n']:>2d} pnl=${w['pnl']:>+6.2f}")


def _ensure_overrides_table(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallet_overrides (
            wallet TEXT PRIMARY KEY,
            tier_override TEXT,
            reason TEXT,
            applied_at BIGINT
        )
    """)


def apply_demotions(conn, candidates: list[dict]) -> tuple[int, list[dict]]:
    """Apply tier downgrade. Returns (count applied, newly-demoted subset).

    The exact column/table for wallet tier varies; we update a generic
    'wallet_overrides' table (created if missing) so downstream logic
    can read it without us having to know which authoritative table
    holds tier. Conservative — we don't modify the leaderboard.

    A candidate is NEWLY demoted when its prior row was absent or
    non-DEMOTED (e.g. a PROBATION wallet that failed re-measurement).
    Only those trigger governance alerts — the nightly re-stamp of an
    already-DEMOTED row stays silent.
    """
    _ensure_overrides_table(conn)
    existing = {
        r[0]: (r[1] or "").upper()
        for r in conn.execute(
            "SELECT wallet, tier_override FROM wallet_overrides").fetchall()
    }
    now = int(time.time())
    n = 0
    newly: list[dict] = []
    for w in candidates:
        conn.execute(
            """INSERT INTO wallet_overrides (wallet, tier_override, reason, applied_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT (wallet) DO UPDATE SET
                 tier_override = EXCLUDED.tier_override,
                 reason = EXCLUDED.reason,
                 applied_at = EXCLUDED.applied_at""",
            (w["wallet"], "DEMOTED",
             f"30d pnl ${w['pnl']:+.2f} on n={w['n']} (auto)",
             now),
        )
        if existing.get(w["wallet"]) != "DEMOTED":
            newly.append(w)
        n += 1
    conn.commit()
    return n, newly


def plan_recoveries(conn, attr_30d: list[dict], candidates: list[dict]) -> dict:
    """Recovery pass for the one-way DEMOTED ratchet (2026-07-16).

    Lifecycle:
      DEMOTED    — live entries blocked; the executor waives $1 execution
                   probes so the 30d window keeps receiving fresh fills
                   (without that carve-out the first demote was an
                   unrecoverable deadlock — no trades, no re-measurement).
      PROBATION  — a DEMOTED wallet that stops meeting the demote criteria
                   (30d pnl above AUTO_DEMOTE_PNL, or n dropped below
                   AUTO_DEMOTE_N because the evidence aged out). The
                   executor allows entries capped at
                   WALLET_PROBATION_STAKE_USD — NOT full size: expired
                   evidence is absence of proof, not proof of recovery.
      (cleared)  — PROBATION row deleted once the 30d window shows
                   pnl >= PROBATION_CLEAR_PNL on n >= AUTO_DEMOTE_N at
                   probation stake. Full-size live resumes.
      re-DEMOTE  — a PROBATION wallet meeting the demote criteria again is
                   re-stamped DEMOTED by apply_demotions (and re-alerted).

    Pure planner — writes nothing. apply_recoveries() commits the plan;
    report-only runs just print it.
    """
    _ensure_overrides_table(conn)
    by_wallet = {w["wallet"]: w for w in attr_30d}
    cand = {w["wallet"] for w in candidates}
    rows = conn.execute(
        "SELECT wallet, tier_override FROM wallet_overrides").fetchall()
    to_probation: list[dict] = []
    to_clear: list[dict] = []
    for wallet, tier in rows:
        tier = (tier or "").upper()
        if wallet in cand or tier not in ("DEMOTED", "PROBATION"):
            continue
        w = by_wallet.get(wallet)
        n = w["n"] if w else 0
        pnl = w["pnl"] if w else 0.0
        if tier == "DEMOTED":
            if n < AUTO_DEMOTE_N:
                why = (f"probation: demote evidence expired "
                       f"(only n={n} resolved in the 30d window)")
            else:
                why = f"probation: 30d pnl recovered to ${pnl:+.2f} on n={n}"
            to_probation.append(
                {"wallet": wallet, "n": n, "pnl": pnl, "why": why})
        else:  # PROBATION
            if n >= AUTO_DEMOTE_N and pnl >= PROBATION_CLEAR_PNL:
                to_clear.append({
                    "wallet": wallet, "n": n, "pnl": pnl,
                    "why": (f"cleared: 30d pnl ${pnl:+.2f} on n={n} "
                            f"at probation stake"),
                })
            # else: keep gathering evidence at probation stake
    return {"to_probation": to_probation, "to_clear": to_clear}


def apply_recoveries(conn, plan: dict) -> int:
    """Write the plan_recoveries() transitions. Returns rows changed."""
    now = int(time.time())
    changed = 0
    for t in plan["to_probation"]:
        conn.execute(
            """UPDATE wallet_overrides
                  SET tier_override = 'PROBATION', reason = ?, applied_at = ?
                WHERE wallet = ?""",
            (t["why"], now, t["wallet"]))
        changed += 1
    for t in plan["to_clear"]:
        conn.execute("DELETE FROM wallet_overrides WHERE wallet = ?",
                     (t["wallet"],))
        changed += 1
    conn.commit()
    return changed


def _esc(v) -> str:
    """Telegram parses messages as HTML and 400s the whole send on a bare
    '<' — exactly how the previous auto-demote alert failed silently for
    3 days. Escape everything dynamic."""
    return html.escape(str(v), quote=False)


def _send_telegram(text: str, *, loud: bool) -> bool:
    """Best-effort Telegram delivery; never raises. Loud messages bypass
    quiet hours / dedup / rate caps at the transport."""
    try:
        from trading_platform.polymarket.telegram_alerts import get_alerter
        sent = bool(get_alerter()._send(text, disable_notification=not loud))
        if not sent:
            print("  (telegram send returned False — not configured or suppressed)")
        return sent
    except Exception as exc:
        print(f"  (telegram send failed: {exc})")
        return False


def _lane_context(conn, wallet: str) -> tuple[list[str], list[str] | None]:
    """(signal types this wallet feeds, other lanes with real non-probe
    orders in the last 7d). Second element is None when the query fails —
    callers must then skip the 'zero live lanes' claim rather than
    asserting it on missing data."""
    try:
        sig_types = [r[0] for r in conn.execute(
            """SELECT DISTINCT signal_type FROM live_trades
                WHERE signal_wallet = ? AND signal_type IS NOT NULL""",
            (wallet,)).fetchall()]
        cutoff = int(time.time()) - 7 * 86400
        others = [r[0] for r in conn.execute(
            """SELECT DISTINCT signal_type FROM live_trades
                WHERE dry_run = 0 AND attempted_at >= ?
                  AND status NOT IN ('blocked', 'error')
                  AND COALESCE(is_probe, 0) = 0
                  AND COALESCE(signal_wallet, '') != ?
                  AND signal_type IS NOT NULL""",
            (cutoff, wallet)).fetchall()]
        return sig_types, others
    except Exception as exc:
        print(f"  (lane-context query failed: {exc})")
        return [], None


def send_governance_alerts(conn, newly_demoted: list[dict], plan: dict) -> None:
    """Surface every override transition to the operator.

    2026-07-16 eval defect #2: demoting a signal-slice pseudo-wallet
    (e.g. phase_b_resolution_decay) is a de-facto strategy kill, but it
    happened as a silent P&L-attribution side-effect. A NEW pseudo-wallet
    demote now sends a loud alert that names the consequence — including
    whether it takes live trading to zero. Recovery transitions alert too,
    so a lane never re-enters live silently either.
    """
    probation_cap = os.environ.get("WALLET_PROBATION_STAKE_USD", "5")

    for w in newly_demoted:
        wallet = w["wallet"]
        if wallet.startswith("0x"):
            continue  # real tracked wallets get the batched line below
        sig_types, others = _lane_context(conn, wallet)
        lines = [
            "\U0001f6d1 <b>STRATEGY SLICE AUTO-DEMOTED</b>",
            f"Slice: <code>{_esc(wallet)}</code>"
            + (f" (signal: {_esc(', '.join(sorted(sig_types)))})" if sig_types else ""),
            f"30d realized: ${w['pnl']:+.2f} on n={w['n']} "
            f"(demote at -${abs(AUTO_DEMOTE_PNL):.0f} on {AUTO_DEMOTE_N}+ trades)",
            "Effect: ALL live entries from this slice are now blocked at "
            "the wallet-override gate; $1 execution probes continue.",
        ]
        if others is not None and not others:
            lines.append(
                "⚠️ No other lane placed a real (non-probe) order "
                "in the last 7d — this demote takes live trading to ZERO.")
        elif others:
            lines.append(
                f"Other live lanes (7d): {_esc(', '.join(sorted(others)))}")
        lines.append(
            f"Recovery: automatic PROBATION (capped ${_esc(probation_cap)}) "
            "once the slice stops meeting the demote criteria; to overrule "
            "now, delete its wallet_overrides row.")
        _send_telegram("\n".join(lines), loud=True)

    real = [w for w in newly_demoted if w["wallet"].startswith("0x")]
    if real:
        body = "\n".join(
            f"  <code>{_esc(w['wallet'][:14])}</code> pnl ${w['pnl']:+.2f} n={w['n']}"
            for w in real[:10])
        _send_telegram(
            f"\U0001f53b <b>{len(real)} wallet(s) auto-demoted</b> "
            f"(30d pnl at or below -${abs(AUTO_DEMOTE_PNL):.0f} "
            f"on {AUTO_DEMOTE_N}+ trades)\n" + body,
            loud=False)

    for t in plan["to_probation"]:
        _send_telegram(
            "\U0001f7e1 <b>OVERRIDE → PROBATION</b>\n"
            f"<code>{_esc(t['wallet'])}</code> — {_esc(t['why'])}\n"
            f"Live entries resume capped at ${_esc(probation_cap)}. Clears "
            f"at 30d pnl of $0 or better on {AUTO_DEMOTE_N}+ trades; "
            "re-demotes if the loss criteria trip again.",
            loud=True)
    for t in plan["to_clear"]:
        _send_telegram(
            "\U0001f7e2 <b>OVERRIDE CLEARED</b>\n"
            f"<code>{_esc(t['wallet'])}</code> — {_esc(t['why'])}\n"
            "Full-size live entries resume.",
            loud=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Apply tier demotions. Default: report only.")
    ap.add_argument("--json", action="store_true",
                    help="JSON output for machine consumption.")
    args = ap.parse_args()

    now = int(time.time())
    conn = get_connection()
    try:
        attr_30d = gather_attribution(conn, now, 30)
        attr_7d = gather_attribution(conn, now, 7)

        if args.json:
            print(json.dumps({"attr_30d": attr_30d, "attr_7d": attr_7d}, default=str))
            return 0

        print_human(attr_30d, attr_7d)

        candidates = [w for w in attr_30d
                      if w["pnl"] <= AUTO_DEMOTE_PNL and w["n"] >= AUTO_DEMOTE_N]
        plan = plan_recoveries(conn, attr_30d, candidates)
        if plan["to_probation"] or plan["to_clear"]:
            print("\n  RECOVERY plan (overrides no longer meeting demote criteria):")
            for t in plan["to_probation"]:
                print(f"    -> PROBATION  {t['wallet'][:50]} — {t['why']}")
            for t in plan["to_clear"]:
                print(f"    -> CLEAR      {t['wallet'][:50]} — {t['why']}")

        if args.apply:
            recovered = apply_recoveries(conn, plan)
            newly: list[dict] = []
            n = 0
            if candidates:
                n, newly = apply_demotions(conn, candidates)
            if n or recovered:
                print(f"\nApplied {n} demotions ({len(newly)} new) + "
                      f"{recovered} recoveries to wallet_overrides table.")
            else:
                print("\nNothing to apply.")
            send_governance_alerts(conn, newly, plan)
    finally:
        try: conn.close()
        except Exception: pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
