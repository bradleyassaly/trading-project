"""Daily system review — the recursive improvement loop.

Pulls together every analytic surface added in the 2026-05-12 audit and
emits a single review report. The pattern is: measure → recommend →
optionally auto-fix → log what changed so the system improves on a
deterministic daily cadence rather than waiting for ad-hoc audits.

Sections:
  1. Yesterday's P&L + position state
  2. Slippage and fill quality (uses fixed fill_price)
  3. Trailing-stop leakage (uses peak_no_price / peak_yes_price)
  4. Resolution-date timing (uses resolution_date)
  5. Order rejection breakdown (FOK, timeouts, etc.)
  6. Calibration check + multiplier refresh
  7. Per-signal alpha decay flags

Run once per UTC day (typically 00:30 UTC) via cron. Recommendations are
printed; only sizing multipliers are auto-applied (low-risk, capped,
direction-aware). Anything else is surfaced for manual review.

Usage:
    python scripts/daily_system_review.py [--apply-multipliers]
"""
import sys
import os
import time
import argparse
import subprocess
import datetime as dt

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
from trading_platform.polymarket.db_connection import get_connection


def section(title: str):
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def section_pnl(conn, now_ts):
    section("1. P&L AND POSITION STATE")
    today_utc = dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    today_start = int(today_utc.timestamp())
    yesterday_start = today_start - 86400

    for label, s, u in [
        ("Yesterday (UTC)", yesterday_start, today_start),
        ("Today so far (UTC)", today_start, now_ts),
        ("Last 7d", now_ts - 7 * 86400, now_ts),
        ("Last 30d", now_ts - 30 * 86400, now_ts),
    ]:
        r = conn.execute("""
            SELECT COUNT(*) n,
                   COALESCE(SUM(realized_pnl), 0) pnl,
                   AVG(CASE WHEN outcome='win' THEN 1.0 ELSE 0.0 END) wr
              FROM live_trades
             WHERE dry_run=0 AND exit_ts >= %s AND exit_ts < %s
               AND realized_pnl IS NOT NULL
        """, (s, u)).fetchone()
        n, pnl, wr = int(r[0] or 0), float(r[1] or 0), float(r[2] or 0)
        print(f"  {label:<22}  n={n:3d}  pnl=${pnl:+8.2f}  WR={wr:.0%}")

    # Open positions snapshot
    open_pos = conn.execute("""
        SELECT COUNT(*) n, COALESCE(SUM(size_usd), 0) capital
          FROM live_trades
         WHERE dry_run=0 AND exit_ts IS NULL
           AND status IN ('submitted','live','matched')
    """).fetchone()
    print(f"  Open positions:        {int(open_pos[0])} positions, "
          f"${float(open_pos[1]):.2f} deployed")


def section_slippage(conn, now_ts):
    section("2. FILL QUALITY (slippage, last 7d)")
    rows = conn.execute("""
        SELECT direction,
               COUNT(*) n,
               AVG(ABS(fill_price - entry_price)) avg_slip,
               AVG(fill_time_ms) avg_ms
          FROM live_trades
         WHERE dry_run=0 AND fill_price IS NOT NULL
           AND entry_price IS NOT NULL AND entry_price > 0
           AND ABS(fill_price - entry_price) < 0.3
           AND attempted_at > %s
         GROUP BY direction
    """, (now_ts - 7 * 86400,)).fetchall()
    for r in rows:
        dire, n, slip, ms = r
        ms_str = f"{float(ms or 0):.0f}ms" if ms else "N/A"
        print(f"  {dire:<5}  n={int(n):3d}  avg slippage={float(slip or 0):.4f}  "
              f"avg fill time={ms_str}")
    if not rows:
        print("  No fills with clean fill_price in last 7d (recent fix; "
              "data accumulates from here)")

    # 2026-06-02: per-(signal,direction) slippage attribution. Surfaces the
    # 47-61% BUY slippage problem at the cell level so we can see WHICH
    # signals are paying spread and which are not. Dollar terms make the
    # leak comparable to realized PnL columns.
    print()
    print("  Per-(signal, direction) slippage attribution (last 30d):")
    rows = conn.execute("""
        SELECT signal_type, direction,
               COUNT(*) n,
               AVG(slippage) avg_slip_pct,
               SUM(slippage * size_usd / NULLIF(fill_price, 0)) slip_dollars
          FROM live_trades
         WHERE dry_run=0 AND slippage IS NOT NULL
           AND fill_price > 0
           AND attempted_at > %s
         GROUP BY signal_type, direction
        HAVING COUNT(*) >= 3
         ORDER BY slip_dollars ASC NULLS LAST
    """, (now_ts - 30 * 86400,)).fetchall()
    if rows:
        print(f"    {'Signal':<28} {'Dir':<5} {'n':>4} {'avg%':>7} {'$ cost':>9}")
        for r in rows:
            sig, dire, n, slip_pct, slip_dol = r
            slip_pct_f = float(slip_pct or 0) * 100
            slip_dol_f = float(slip_dol or 0)
            print(f"    {str(sig):<28} {str(dire):<5} {int(n):>4} "
                  f"{slip_pct_f:>+6.1f}% ${slip_dol_f:>+7.2f}")

    # Worst per-trade offenders — these are where the dollars actually leaked
    print()
    print("  Top-10 worst per-trade slippage hits (last 30d):")
    rows = conn.execute("""
        SELECT id, signal_type, direction,
               entry_price, fill_price, slippage,
               size_usd,
               slippage * size_usd / NULLIF(fill_price, 0) slip_dollars,
               LEFT(question, 40) q
          FROM live_trades
         WHERE dry_run=0 AND slippage IS NOT NULL
           AND fill_price > 0 AND size_usd > 0
           AND attempted_at > %s
         ORDER BY slip_dollars ASC NULLS LAST
         LIMIT 10
    """, (now_ts - 30 * 86400,)).fetchall()
    if rows:
        print(f"    {'#id':<7} {'sig':<22} {'dir':<5} {'entry→fill':<14} "
              f"{'sz':>6} {'cost':>8}")
        for r in rows:
            tid, sig, dire, ep, fp, slip, sz, dol, q = r
            ep_f = float(ep or 0); fp_f = float(fp or 0)
            print(f"    #{int(tid):<6} {str(sig):<22} {str(dire):<5} "
                  f"{ep_f:.3f}→{fp_f:.3f}  ${float(sz or 0):>5.1f} "
                  f"${float(dol or 0):>+6.2f}")
            print(f"         {str(q)}")


def section_trailing_leak(conn, now_ts):
    section("3. TRAILING-STOP LEAKAGE (peak vs exit, last 14d)")
    rows = conn.execute("""
        SELECT signal_type, direction,
               COUNT(*) n,
               AVG(peak_no_price - (1 - COALESCE(exit_price, entry_price))) leak_no,
               AVG(peak_yes_price - COALESCE(exit_price, entry_price)) leak_yes
          FROM live_trades
         WHERE dry_run=0 AND exit_reason = 'trailing_stop'
           AND (peak_no_price IS NOT NULL OR peak_yes_price IS NOT NULL)
           AND attempted_at > %s
         GROUP BY signal_type, direction
    """, (now_ts - 14 * 86400,)).fetchall()
    for r in rows:
        sig, dire, n, leak_no, leak_yes = r
        leak = float(leak_no or 0) if dire == "SELL" else float(leak_yes or 0)
        print(f"  {str(sig):<28} {str(dire):<5}  n={int(n):3d}  "
              f"avg ${leak:.3f}/share left on table")
    if not rows:
        print("  No data yet (peak_no/peak_yes_price only populates from "
              "positions opened after 2026-05-12)")


def section_resolution(conn, now_ts):
    section("4. RESOLUTION-DATE TIMING (positions held vs days-to-resolution)")
    rows = conn.execute("""
        SELECT signal_type, direction,
               COUNT(*) n,
               AVG((resolution_date - attempted_at)/86400.0) avg_days_out,
               AVG(CASE WHEN outcome='win' THEN 1.0 ELSE 0.0 END) wr
          FROM live_trades
         WHERE dry_run=0 AND resolution_date IS NOT NULL
           AND outcome IN ('win','loss')
           AND attempted_at > %s
         GROUP BY signal_type, direction
    """, (now_ts - 30 * 86400,)).fetchall()
    for r in rows:
        sig, dire, n, days, wr = r
        print(f"  {str(sig):<28} {str(dire):<5}  n={int(n):3d}  "
              f"avg {float(days or 0):.1f}d to resolution  WR={float(wr or 0):.0%}")
    if not rows:
        print("  No data yet (resolution_date only populates from entries "
              "after 2026-05-12)")


def section_rejections(conn, now_ts):
    section("5. ORDER REJECTIONS (last 7d)")
    rows = conn.execute("""
        SELECT
            CASE WHEN error_msg LIKE '%%timed out%%' THEN 'timeout'
                 WHEN error_msg LIKE '%%FOK%%' THEN 'FOK'
                 WHEN error_msg LIKE '%%couldn%%fully filled%%' THEN 'FOK-fill'
                 WHEN error_msg LIKE '%%invalid amount%%' THEN 'min-stake'
                 WHEN error_msg LIKE '%%invalid signature%%' THEN 'sig'
                 WHEN error_msg LIKE '%%thin liquidity%%' THEN 'thin-liq'
                 ELSE 'other'
            END class,
            direction,
            COUNT(*) n
          FROM live_trades
         WHERE dry_run=0 AND status='error' AND attempted_at > %s
         GROUP BY class, direction ORDER BY n DESC
    """, (now_ts - 7 * 86400,)).fetchall()
    if not rows:
        print("  No rejections in last 7d")
    for r in rows:
        print(f"  {str(r[0]):<10} {str(r[1]):<5}  n={int(r[2])}")


def section_calibration(conn, args):
    section("6. CALIBRATION CHECK + MULTIPLIER REFRESH")
    # Pull current multipliers
    cur = conn.execute("""
        SELECT signal_type, direction, multiplier, n_resolved,
               actual_wr, mean_confidence, updated_at
          FROM signal_sizing_multipliers
         ORDER BY updated_at DESC, signal_type, direction
    """).fetchall()
    if cur:
        print(f"  Currently applied multipliers:")
        print(f"    {'Signal':<28} {'Dir':<5} {'mult':>6} {'WR':>7} {'conf':>7} {'age_h':>6}")
        for r in cur:
            age_h = (time.time() - int(r[6] or 0)) / 3600
            print(f"    {str(r[0]):<28} {str(r[1]):<5} {float(r[2]):>5.2f}x "
                  f"{float(r[4] or 0):>6.1%} {float(r[5] or 0):>6.3f} "
                  f"{age_h:>5.1f}h")
    else:
        print("  No multipliers in table yet")

    # 2026-06-18: calibration-vs-multiplier convergence tracker. Per-slice
    # isotonic calibration now drives Kelly's confidence (ENABLE_PER_SLICE_CALIB
    # + executor passes signal_type/direction). As confidence calibrates, the
    # multiplier (= actual_WR / mean_confidence) should drift toward 1.0 — that
    # is the signal the band-aid is retiring. This block surfaces the drift so
    # we can confirm convergence before removing the multiplier read entirely.
    conv = conn.execute("""
        SELECT lt.signal_type, lt.direction,
               COUNT(*) n,
               AVG(lt.confidence) mean_conf,
               AVG(CASE WHEN lt.outcome='win' THEN 1.0 ELSE 0.0 END) wr,
               MAX(m.multiplier) mult
          FROM live_trades lt
          LEFT JOIN signal_sizing_multipliers m
                 ON m.signal_type = lt.signal_type AND m.direction = lt.direction
         WHERE lt.dry_run=0 AND lt.outcome IN ('win','loss')
           AND lt.confidence IS NOT NULL
           AND lt.exit_ts >= EXTRACT(EPOCH FROM NOW())::BIGINT - 30*86400
         GROUP BY 1,2
        HAVING COUNT(*) >= 8
         ORDER BY 3 DESC
    """).fetchall()
    if conv:
        print()
        print("  Calib→multiplier convergence (30d; mult should approach 1.0):")
        print(f"    {'Signal':<24}{'Dir':<5}{'n':>3} {'calibConf':>9} {'WR':>6} "
              f"{'mult':>6}  status")
        for r in conv:
            sig, di, n, mc, wr, mult = r
            mc = float(mc or 0); wr = float(wr or 0)
            mult = float(mult) if mult is not None else 1.0
            # Converged when the live multiplier is within ~20% of 1.0 — i.e.
            # calibrated confidence already matches realized WR, band-aid idle.
            status = "converged" if 0.8 <= mult <= 1.25 else "correcting"
            gap = wr - mc  # residual miscalibration the multiplier still patches
            print(f"    {str(sig):<24}{str(di):<5}{int(n):>3} {mc:>9.3f} "
                  f"{wr:>6.0%} {mult:>5.2f}x  {status} (WR−conf={gap:+.2f})")

    if args.apply_multipliers:
        print()
        print("  Refreshing multipliers from latest data...")
        sp = subprocess.run(
            [sys.executable,
             os.path.join(os.path.dirname(__file__), "update_sizing_multipliers.py"),
             "--days", "30"],
            capture_output=True, text=True,
        )
        for line in sp.stdout.splitlines()[-12:]:
            print(f"    {line}")
    else:
        print()
        print("  (use --apply-multipliers to refresh; skipped this run)")


def section_decay(conn, now_ts):
    section("7. ALPHA DECAY FLAGS (per-signal 7d vs 30d WR)")
    rows = conn.execute("""
        SELECT signal_type, direction,
               AVG(CASE WHEN exit_ts > %s AND outcome='win' THEN 1.0
                        WHEN exit_ts > %s THEN 0.0 ELSE NULL END) wr_7d,
               COUNT(CASE WHEN exit_ts > %s AND outcome IN ('win','loss') THEN 1 END) n_7d,
               AVG(CASE WHEN outcome='win' THEN 1.0 WHEN outcome='loss' THEN 0.0 END) wr_30d,
               COUNT(CASE WHEN outcome IN ('win','loss') THEN 1 END) n_30d
          FROM live_trades
         WHERE dry_run=0 AND attempted_at > %s
         GROUP BY signal_type, direction
         HAVING COUNT(CASE WHEN exit_ts > %s AND outcome IN ('win','loss') THEN 1 END) >= 3
    """, (now_ts - 7*86400, now_ts - 7*86400, now_ts - 7*86400,
          now_ts - 30*86400, now_ts - 7*86400)).fetchall()
    for r in rows:
        sig, dire, wr7, n7, wr30, n30 = r
        wr7 = float(wr7 or 0); wr30 = float(wr30 or 0)
        decay = wr30 - wr7
        flag = ""
        if n7 >= 5 and decay > 0.20:
            flag = "  DECAY — 7d WR dropped >20pp vs 30d"
        elif n7 >= 5 and -decay > 0.20:
            flag = "  IMPROVING — 7d WR up >20pp vs 30d"
        print(f"  {str(sig):<28} {str(dire):<5} "
              f"7d={wr7:.0%}(n={int(n7)})  30d={wr30:.0%}(n={int(n30)})  "
              f"delta={decay:+.2f}{flag}")


def section_attribution(conn, now_ts):
    """Per-(signal × direction) revenue attribution over 7d and 30d.

    Surfaces signal monoculture risk and decay-by-strategy. As of 2026-05-19
    wallet_reversal SELL drove ~70% of weekly revenue — that's the kind of
    concentration this section is designed to flag.
    """
    section("9. PER-STRATEGY ATTRIBUTION (revenue share by signal × direction)")
    for label, days in (("Last 7d", 7), ("Last 30d", 30)):
        cutoff = now_ts - days * 86400
        rows = conn.execute("""
            SELECT signal_type, direction,
                   COUNT(*) n,
                   SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
                   ROUND(SUM(COALESCE(realized_pnl, 0))::numeric, 2) pnl
              FROM live_trades
             WHERE dry_run=0 AND exit_ts >= %s
               AND realized_pnl IS NOT NULL
             GROUP BY 1, 2
             ORDER BY 5 DESC
        """, (cutoff,)).fetchall()
        if not rows:
            print(f"  {label}: no resolved trades")
            continue
        total = sum(float(r[4] or 0) for r in rows)
        total_abs = sum(abs(float(r[4] or 0)) for r in rows)
        print(f"\n  {label} — total realized: ${total:.2f}")
        if total_abs <= 0:
            continue
        for r in rows:
            sig, di, n, wins, pnl = r
            wr = (wins / max(n, 1)) * 100
            share = abs(float(pnl)) / total_abs * 100
            bar = "█" * min(int(share / 5), 14)  # 14-char max bar
            print(f"    {sig:<22s} {di:<5s} n={n:3d} WR={wr:3.0f}% "
                  f"pnl=${float(pnl):>+7.2f}  {share:>4.0f}% {bar}")
        # Monoculture flag
        if rows and total_abs > 0:
            top_share = abs(float(rows[0][4])) / total_abs
            if top_share >= 0.50:
                print(f"  MONOCULTURE — {rows[0][0]} {rows[0][1]} drives "
                      f"{top_share:.0%} of |realized PnL|; add an uncorrelated signal.")


def section_stale_equity(conn, now_ts):
    """Detect frozen equity snapshots — a sign of stale balance-API caching.

    On 2026-05-18, the live_equity snapshot reported tokens=$5.43 for 15+
    consecutive runs while real on-chain SELL positions held $60+. Root
    cause: py_clob_client_v2 connection pool returned stale balances after
    the container had been up for weeks. Restart cleared it. This check
    flags it on a daily cadence so we catch a recurrence within hours,
    not days.
    """
    section("8. EQUITY-SNAPSHOT FRESHNESS (stale-balance detector)")
    # Detect frozen open_market_value across consecutive snapshots — the
    # signature of the 2026-05-18 stale-balance-API incident.
    #
    # Tuning (refined 2026-05-20 after first false positive):
    #   - Threshold raised 5 → 10 consecutive identical snapshots.
    #     Near-resolved SELL positions (YES ≈ 0.001) genuinely don't move
    #     for hours; 5 was too sensitive. 10 ≈ 10h of static value, which
    #     is only suspicious if real fills happened in that window.
    #   - The "stale API" call-to-action only fires when there were actual
    #     FILLS in the window (not just attempts/blocks). Fills change
    #     on-chain conditional balances; if balance API returns the same
    #     mkt value across 10+ snapshots with new fills, the API is stale.
    #   - Without recent fills, the cluster is informational only — likely
    #     just a low-vol period on near-resolved positions.
    cutoff = now_ts - 86400
    THRESHOLD = 10
    rows = conn.execute("""
        SELECT open_market_value, COUNT(*) n
          FROM live_equity_snapshots
         WHERE ts >= %s
         GROUP BY open_market_value
        HAVING COUNT(*) >= %s
         ORDER BY n DESC LIMIT 3
    """, (cutoff, THRESHOLD)).fetchall()
    if not rows:
        print(f"  OK — no clusters of {THRESHOLD}+ identical snapshots in 24h.")
    else:
        # FILLS (state-changing on-chain events), not just attempts/blocks
        fills = conn.execute("""
            SELECT COUNT(*) FROM live_trades
             WHERE dry_run=0 AND attempted_at >= %s
               AND status IN ('matched','live')
        """, (cutoff,)).fetchone()[0]
        exits = conn.execute("""
            SELECT COUNT(*) FROM live_trades
             WHERE dry_run=0 AND exit_ts >= %s
               AND realized_pnl IS NOT NULL
        """, (cutoff,)).fetchone()[0]
        actionable = (fills + exits) >= 2
        prefix = "WARNING" if actionable else "INFO   "
        print(f"  {prefix} — open_market_value frozen across snapshots:")
        for r in rows:
            print(f"    tokens=${float(r[0] or 0):.2f}  repeated {r[1]} times")
        print(f"  Window activity: {fills} fills + {exits} exits in last 24h")
        if actionable:
            print(f"  ACTION — fills occurred but mkt frozen → balance API likely stale.")
            print(f"  Run: docker compose restart scheduler live-collect")
        else:
            print(f"  No-op — low fill activity makes static value plausible "
                  f"(near-resolved SELL positions don't move).")


def section_api_health(conn, now_ts):
    """External-API health: surface any silent dependency early.

    2026-05-23: built after the Gamma 422 incident silently killed live
    signal flow for 24h. The pattern (stale balance API hid $57; Gamma
    422 dropped 7 SELL/day) shows external dependencies degrade silently
    and we notice only when realized PnL deviates. This section flags
    any tracked API whose last success is older than the freshness
    threshold for that API.
    """
    section("11. EXTERNAL-API HEALTH (silent-dependency detector)")
    try:
        from trading_platform.polymarket.api_health import get_status
        rows = get_status()
    except Exception as exc:
        print(f"  api_health module error: {exc}")
        return
    if not rows:
        print("  No api_health rows yet — instrumentation hasn't fired since deploy.")
        return
    # Per-API freshness thresholds (seconds).
    THRESHOLDS = {
        "gamma": 600,           # markets/midpoint should be every few min
        "clob": 600,            # balance/order endpoints — every few min
        "data_api_trades": 1800,  # wallet poller is 10m cadence
        "polymarket_other": 3600,
    }
    any_stale = False
    for r in rows:
        thr = THRESHOLDS.get(r["api_name"], 3600)
        age = r["last_success_age_sec"]
        age_s = f"{age}s" if age is not None and age < 120 else (f"{age//60}m" if age else "never")
        flag = ""
        if age is None or age > thr:
            flag = f"  STALE — silent {age_s} (threshold {thr}s)"
            any_stale = True
        elif r["error_24h"] > 0:
            flag = f"  errors_24h={r['error_24h']}"
        print(f"  {r['api_name']:<22s} last_success={age_s:>7s}  "
              f"24h: {r['success_24h']} ok / {r['error_24h']} err{flag}")
    if any_stale:
        print()
        print("  ACTION — at least one API is silent. Investigate before silent volume loss.")
        print("  (Most likely: external API changed semantics; check container logs for HTTP errors.)")


def section_polymarket_reconcile(conn, now_ts):
    """Surface drift between DB and on-chain Polymarket truth.

    Reads the latest reconcile_polymarket.log written by the scheduled
    reconciler (every 4h). Lists any DRIFT lines so the operator sees
    persistent disagreements between DB cash/positions and on-chain
    reality without having to grep logs.
    """
    section("10. POLYMARKET-TRUTH RECONCILIATION (last drift report)")
    import os
    log_path = "/app/logs/scheduler/reconcile_polymarket_truth.log"
    if not os.path.exists(log_path):
        # Fallback for local runs
        log_path = "logs/scheduler/reconcile_polymarket_truth.log"
    if not os.path.exists(log_path):
        print("  No reconciler log yet — task may not have fired since deploy.")
        return
    # Read backwards to find the last "===" block (most recent run)
    try:
        with open(log_path, "r") as f:
            lines = f.readlines()
        if not lines:
            print("  Reconciler log empty.")
            return
        # Find last "===" header
        last_hdr = None
        for i in range(len(lines) - 1, -1, -1):
            if lines[i].startswith("=== Polymarket-truth reconciliation"):
                last_hdr = i
                break
        if last_hdr is None:
            print("  No reconciliation block found in log.")
            return
        block = lines[last_hdr:]
        # Trim to 30 lines max so a verbose drift doesn't blow up the review
        for ln in block[:30]:
            print(f"  {ln.rstrip()}")
    except Exception as exc:
        print(f"  Could not read reconciler log: {exc}")


def section_equity_agreement(conn, now_ts):
    """Phase-0 exit gate: on-chain equity and DB equity must agree.

    SCALING_PLAN_2026-07-02.md Phase 0 requires 14 consecutive days of
    on-chain/DB equity agreement (within $1 cash drift and no share
    drifts) before any bankroll promotion. This section runs the
    reconciler in-process, records today's PASS/FAIL in
    equity_agreement_log, and prints the running streak so the operator
    can see exactly where the 14-day clock stands.
    """
    section("12. EQUITY AGREEMENT — PHASE-0 EXIT GATE (14-day streak)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS equity_agreement_log (
            day          TEXT PRIMARY KEY,
            ts           BIGINT NOT NULL,
            db_usdc      DOUBLE PRECISION,
            onchain_usdc DOUBLE PRECISION,
            usdc_diff    DOUBLE PRECISION,
            n_drifts     BIGINT NOT NULL,
            n_errors     BIGINT NOT NULL,
            passed       BIGINT NOT NULL
        )
    """)
    today = dt.datetime.utcnow().strftime("%Y-%m-%d")
    try:
        from reconcile_polymarket_truth import reconcile
        report = reconcile()
    except Exception as exc:
        print(f"  Reconciler unavailable ({str(exc)[:80]}) — recording FAIL.")
        report = {"drifts": [{"metric": "reconciler_error"}],
                  "ok": [], "errors": [str(exc)[:200]]}

    drifts = report.get("drifts", [])
    errors = report.get("errors", [])
    usdc_row = next((e for e in drifts + report.get("ok", [])
                     if e.get("metric") == "usdc_balance"), {})
    # Errors count as failure: "could not verify" is not agreement.
    passed = 1 if (not drifts and not errors) else 0
    conn.execute("""
        INSERT INTO equity_agreement_log
            (day, ts, db_usdc, onchain_usdc, usdc_diff, n_drifts, n_errors, passed)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (day) DO UPDATE SET
            ts = EXCLUDED.ts, db_usdc = EXCLUDED.db_usdc,
            onchain_usdc = EXCLUDED.onchain_usdc,
            usdc_diff = EXCLUDED.usdc_diff, n_drifts = EXCLUDED.n_drifts,
            n_errors = EXCLUDED.n_errors, passed = EXCLUDED.passed
    """, (today, now_ts, usdc_row.get("db"), usdc_row.get("onchain"),
          usdc_row.get("diff"), len(drifts), len(errors), passed))
    conn.commit()

    rows = conn.execute("""
        SELECT day, passed, usdc_diff, n_drifts, n_errors
          FROM equity_agreement_log ORDER BY day DESC LIMIT 20
    """).fetchall()
    streak = 0
    for r in rows:
        if int(r[1]) == 1:
            streak += 1
        else:
            break
    status = "PASS" if passed else "FAIL"
    diff_s = f"${usdc_row['diff']:+.2f}" if usdc_row.get("diff") is not None else "n/a"
    print(f"  Today: {status}  (usdc diff {diff_s}, "
          f"{len(drifts)} drift(s), {len(errors)} error(s))")
    print(f"  Streak: {streak}/14 consecutive agreeing days"
          + ("  ✅ GATE MET" if streak >= 14 else ""))
    for r in rows[:7]:
        print(f"    {r[0]}  {'PASS' if int(r[1]) else 'FAIL'}"
              f"  drifts={int(r[3])} errors={int(r[4])}")


def section_reconciled_ev(conn, now_ts):
    """Phase-0: per-signal EV verdicts split by exit-truth channel.

    The 2026-05-27 audit showed action-triggered exits (take_profit /
    trailing_stop / stop_loss / ...) can book fictitious P&L when the
    exit order rests unfilled, while resolution exits settle on-chain
    and cannot lie. Splitting EV by channel bounds how much of each
    signal's measured edge could be contamination. Verdicts:
      GO    — EV > 0 on both channels, n >= 30 total
      PAPER — insufficient n, or channels disagree on sign
      KILL  — EV < 0 on n >= 30
    """
    section("13. RECONCILED PER-SIGNAL EV VERDICTS (90d, by exit channel)")
    rows = conn.execute("""
        SELECT signal_type,
               CASE WHEN exit_reason IN ('resolved_zero_balance', 'redeemed',
                                         'resolution', 'market_resolved')
                    THEN 'resolution' ELSE 'action' END AS channel,
               COUNT(*) AS n,
               COALESCE(SUM(realized_pnl), 0) AS pnl,
               COALESCE(SUM(size_usd), 0) AS staked
          FROM live_trades
         WHERE dry_run = 0 AND exit_ts IS NOT NULL
           AND outcome IN ('win', 'loss')
           AND exit_ts >= %s
         GROUP BY signal_type, 2
         ORDER BY signal_type
    """, (now_ts - 90 * 86400,)).fetchall()
    by_sig: dict = {}
    for r in rows:
        sig, ch, n, pnl, staked = r[0], r[1], int(r[2]), float(r[3]), float(r[4])
        d = by_sig.setdefault(sig, {})
        d[ch] = {"n": n, "pnl": pnl, "ev": (pnl / staked) if staked else 0.0}
    if not by_sig:
        print("  No closed live trades in window.")
        return
    print(f"  {'signal':<24} {'n_act':>5} {'ev_act':>8} {'n_res':>5} "
          f"{'ev_res':>8} {'total_pnl':>10}  verdict")
    for sig, d in sorted(by_sig.items()):
        act = d.get("action", {"n": 0, "pnl": 0.0, "ev": 0.0})
        res = d.get("resolution", {"n": 0, "pnl": 0.0, "ev": 0.0})
        n_total = act["n"] + res["n"]
        pnl_total = act["pnl"] + res["pnl"]
        evs = [c["ev"] for c in (act, res) if c["n"] > 0]
        if n_total >= 30 and evs and all(e > 0 for e in evs):
            verdict = "GO"
        elif n_total >= 30 and evs and all(e < 0 for e in evs):
            verdict = "KILL"
        else:
            verdict = "PAPER"
        if act["n"] > 0 and res["n"] > 0 and (act["ev"] > 0) != (res["ev"] > 0):
            verdict += " (channels disagree — audit fills)"
        print(f"  {sig:<24} {act['n']:>5} {act['ev']:>+7.1%} {res['n']:>5} "
              f"{res['ev']:>+7.1%} {pnl_total:>+9.2f}$  {verdict}")


def section_detection_latency(conn, now_ts):
    """Phase-2 measurement: whale fill → our order attempt, per signal.

    For copy-trading, latency IS the edge. The 04-24 audit measured a
    9-min median tier-1 lag against a 2-5s design goal; this section
    makes the number visible per trade so the CTFExchange-subscription
    work (Phase 2 item 1) has a before/after baseline. Includes dry-run
    attempts — they traverse the same detection path.
    """
    section("14. DETECTION LATENCY (whale fill → our attempt, last 7d)")
    rows = conn.execute("""
        SELECT signal_type,
               COUNT(*) AS n,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY detection_latency_sec) AS p50,
               PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY detection_latency_sec) AS p90,
               MAX(detection_latency_sec) AS worst
          FROM live_trades
         WHERE detection_latency_sec IS NOT NULL
           AND attempted_at >= %s
         GROUP BY signal_type
         ORDER BY n DESC
    """, (now_ts - 7 * 86400,)).fetchall()
    if not rows:
        print("  No latency-instrumented attempts yet (column ships 2026-07-02;")
        print("  data accumulates from the next deploy).")
        return
    print(f"  {'signal':<26} {'n':>5} {'p50':>8} {'p90':>8} {'worst':>8}")
    for r in rows:
        sig, n, p50, p90, worst = r[0], int(r[1]), r[2], r[3], r[4]
        fmt = lambda v: f"{v/60:.1f}m" if v and v >= 120 else (f"{v:.0f}s" if v is not None else "n/a")
        flag = "  ⚠ >60s" if p50 and p50 > 60 else ""
        print(f"  {sig:<26} {n:>5} {fmt(p50):>8} {fmt(p90):>8} {fmt(worst):>8}{flag}")
    print("  Target (Phase-2 exit gate): p50 ≤ 10s on the whale-copy lane.")


def section_tail_concentration(conn, now_ts):
    """P2 flaw #9 measurement: how short-vol is the open book right now.

    The resolution_decay lottery lane has an insurance-like payoff —
    many small wins, rare large correlated losses. Before adding hard
    per-event caps we need to SEE the concentration: open exposure by
    entry-price band and by category, plus the single largest positions.
    """
    section("15. TAIL CONCENTRATION (open live positions)")
    rows = conn.execute("""
        SELECT CASE
                 WHEN signal_price >= 0.90 OR signal_price <= 0.10
                   THEN 'near-certainty (<=0.10 or >=0.90)'
                 WHEN signal_price >= 0.70 OR signal_price <= 0.30
                   THEN 'skewed (0.10-0.30 / 0.70-0.90)'
                 ELSE 'mid (0.30-0.70)'
               END AS band,
               COUNT(*) AS n,
               COALESCE(SUM(size_usd), 0) AS at_risk
          FROM live_trades
         WHERE dry_run = 0 AND exit_ts IS NULL
           AND status IN ('submitted', 'live', 'matched')
         GROUP BY 1 ORDER BY at_risk DESC
    """).fetchall()
    if not rows:
        print("  No open live positions.")
        return
    total = sum(float(r[2]) for r in rows)
    for r in rows:
        band, n, risk = r[0], int(r[1]), float(r[2])
        pct = risk / total if total else 0
        print(f"  {band:<38} n={n:<3} ${risk:>8.2f}  ({pct:.0%} of open book)")
    cats = conn.execute("""
        SELECT COALESCE(category, '?'), COUNT(*), COALESCE(SUM(size_usd), 0)
          FROM live_trades
         WHERE dry_run = 0 AND exit_ts IS NULL
           AND status IN ('submitted', 'live', 'matched')
         GROUP BY 1 ORDER BY 3 DESC LIMIT 5
    """).fetchall()
    print("  By category:")
    for r in cats:
        print(f"    {r[0]:<20} n={int(r[1]):<3} ${float(r[2]):.2f}")
    big = conn.execute("""
        SELECT question, size_usd, signal_price
          FROM live_trades
         WHERE dry_run = 0 AND exit_ts IS NULL
           AND status IN ('submitted', 'live', 'matched')
         ORDER BY size_usd DESC NULLS LAST LIMIT 3
    """).fetchall()
    print("  Largest single positions:")
    for r in big:
        q = (r[0] or "?")[:60]
        print(f"    ${float(r[1] or 0):>7.2f} @ {r[2] if r[2] is not None else '?'}  {q}")


def section_copy_benchmark(conn, now_ts):
    """Us vs naive copy — is the system earning its complexity?

    Runs the per-wallet naive-copy backtest (wallet_trades resolution
    enrichment, same filters as the naive_copy shadow lane, ex-top-3 EV
    gate) and prints the head-to-head. If naive-copying some wallet ever
    beats the system on ex-top-3 EV, that wallet is the new benchmark
    the system has to justify itself against — or we just copy it.
    """
    section("16. COPY BENCHMARK (system vs per-wallet naive copy, 30d)")
    try:
        from trading_platform.polymarket.naive_copy_backtest import run_backtest
        out = run_backtest(days=30, min_n=15)
    except Exception as exc:
        print(f"  backtest failed: {exc}")
        return
    s = out["our_system"]
    print(f"  Our system:  n={s['n_closed']}  pnl=${s['realized_pnl']:+.2f}  "
          f"per-$={s['pnl_per_dollar'] if s['pnl_per_dollar'] is not None else 'n/a'}  "
          f"WR={s['wr'] if s['wr'] is not None else 'n/a'}")
    print(f"  Copyable wallets (n>={out['min_n']}, WR>={out['min_wr']:.0%}, "
          f"ex-top-3 EV>0): {len(out['qualified'])} of {out['wallets_with_min_n']} tested")
    for r in out["qualified"][:5]:
        print(f"    {r['wallet'][:14]}  n={r['n']}  WR={r['wr']:.0%}  "
              f"exTop3=${r['ex_top3_ev']:+.3f}/tr  {r['top_category']}")
    if not out["qualified"]:
        best = out["top20"][0] if out["top20"] else None
        if best:
            print(f"    none qualify — best candidate {best['wallet'][:14]} "
                  f"exTop3=${best['ex_top3_ev']:+.3f}/tr (needs > 0)")
    sh = out["shadow_lane"]
    print(f"  Shadow lane actuals: n={sh['n_resolved']} "
          f"pnl=${sh['realized_pnl']:+.2f} "
          f"WR={sh['wr'] if sh['wr'] is not None else 'n/a'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply-multipliers", action="store_true",
                    help="Refresh signal_sizing_multipliers from latest data")
    args = ap.parse_args()

    now_ts = int(time.time())
    today_str = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    print(f"\nDAILY SYSTEM REVIEW — {today_str} UTC")

    conn = get_connection()
    try:
        section_pnl(conn, now_ts)
        section_slippage(conn, now_ts)
        section_trailing_leak(conn, now_ts)
        section_resolution(conn, now_ts)
        section_rejections(conn, now_ts)
        section_calibration(conn, args)
        section_decay(conn, now_ts)
        section_attribution(conn, now_ts)
        section_stale_equity(conn, now_ts)
        section_polymarket_reconcile(conn, now_ts)
        section_api_health(conn, now_ts)
        section_equity_agreement(conn, now_ts)
        section_reconciled_ev(conn, now_ts)
        section_detection_latency(conn, now_ts)
        section_tail_concentration(conn, now_ts)
        section_copy_benchmark(conn, now_ts)
    finally:
        conn.close()

    print()
    print("=" * 72)
    print("END OF REVIEW")
    print("=" * 72)


if __name__ == "__main__":
    main()
