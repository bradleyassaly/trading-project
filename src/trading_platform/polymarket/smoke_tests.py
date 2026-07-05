"""Pre-deploy smoke tests — catches issues that production-only would.

Lessons from 2026-04-30 → 2026-05-01:
  - market_ticks schema mismatch silently broke 4 strategies (caught
    only in prod after a day)
  - kalshi_ingest scope too large → 40 consecutive timeouts overnight
  - Calibration curve degeneracy (bp_y range collapsed to 0.5) ran
    for days before being noticed
  - whale_entry was tier-A by Brier but -EV by backtest

Each lesson translates to a smoke check below. Run via:
    python -m trading_platform.polymarket.smoke_tests
or wire into the scheduler at deploy-cadence.

Each check returns (name, passed: bool, detail: str). Non-blocking.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


def _check_market_ticks_schema(conn) -> tuple[bool, str]:
    """Verify the canonical market_ticks columns exist.
    2026-04-30: yesterday's strategies wrote (ts, yes_price) — wrong.
    """
    try:
        cols = {r[0] for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='market_ticks'"
        ).fetchall()}
        required = {"condition_id", "token_id", "timestamp", "price"}
        missing = required - cols
        if missing:
            return False, f"missing columns: {sorted(missing)}"
        return True, f"all canonical columns present (cols={sorted(cols)})"
    except Exception as exc:
        return False, str(exc)[:120]


def _check_kalshi_ingest_pace(conn) -> tuple[bool, str]:
    """Verify kalshi_ingest task isn't in sustained failure.
    2026-05-01: 40 consecutive timeouts went unnoticed for 7h before
    we noticed system was NOT_READY.
    2026-07-05: kalshi_ingest was deliberately disabled 2026-06-03
    (cross-platform arb is dead), so an empty last-hour count is the
    EXPECTED state — this check failed on every run for a month and
    trained us to ignore the smoke suite. Pass while ingestion is off;
    re-arm via KALSHI_INGEST_ENABLED=1 if the arb lane ever returns.
    """
    if os.environ.get("KALSHI_INGEST_ENABLED", "0") != "1":
        return True, "kalshi ingest disabled (arb lane retired 2026-06-03) — skipped"
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM kalshi_markets "
            "WHERE last_updated > extract(epoch FROM NOW() - interval '1 hour')::bigint"
        ).fetchone()[0]
        if n < 100:
            return False, f"only {n} kalshi rows updated in last hour"
        return True, f"{n} kalshi rows updated in last hour (healthy)"
    except Exception as exc:
        return False, str(exc)[:120]


def _check_calibration_curve_health(conn) -> tuple[bool, str]:
    """Verify the live calibration curve isn't degenerate.
    2026-05-01: discovered the live curve had bp_y collapsed to
    [0, 0.50] — every input mapped to ≤50%, killing all signals.
    """
    try:
        row = conn.execute(
            "SELECT breakpoints_json FROM alpha_calibration_curve "
            "WHERE category IS NULL ORDER BY fitted_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            return True, "no global curve fit yet (identity fallback safe)"
        import json as _json
        bps = _json.loads(row[0])
        ys = [float(b["y"]) for b in bps]
        if not ys:
            return False, "curve has no breakpoints"
        bp_range = max(ys) - min(ys)
        bp_max = max(ys)
        if bp_range < 0.30:
            return False, f"DEGENERATE curve (bp_y range={bp_range:.2f} < 0.30)"
        if bp_max < 0.65:
            # Curve compresses high-confidence inputs — high-conf signals
            # post-calibration can't beat 0.65, killing every gate that
            # needs conf > 0.5.
            return False, f"COMPRESSED curve (bp_y max={bp_max:.2f} < 0.65 — kills high-conf signals)"
        return True, f"curve healthy (bp_y range={bp_range:.2f}, max={bp_max:.2f})"
    except Exception as exc:
        return False, str(exc)[:120]


def _check_signal_tier_vs_backtest(conn) -> tuple[bool, str]:
    """Verify SIGNAL_CALIBRATION_TIER doesn't promote a -EV signal.
    2026-04-30: whale_entry was Tier A by Brier but -30.9% EV. Re-tier
    must reflect EV, not Brier alone.
    """
    try:
        from trading_platform.polymarket.polymarket_paper_executor import SIGNAL_CALIBRATION_TIER
        # Pull most-recent backtest stats per signal
        rows = conn.execute(
            """SELECT signal_type, ev FROM signal_backtest_results sb
                WHERE run_ts = (SELECT MAX(run_ts) FROM signal_backtest_results
                                WHERE signal_type = sb.signal_type)
                  AND ev IS NOT NULL"""
        ).fetchall()
        violations = []
        for st, ev in rows:
            tier = SIGNAL_CALIBRATION_TIER.get(st)
            if tier == "A" and ev is not None and float(ev) < 0.05:
                violations.append(f"{st}={float(ev)*100:+.0f}%")
        if violations:
            return False, f"Tier-A signals with EV<5%: {', '.join(violations)}"
        return True, "all Tier-A signals have backtest EV ≥ 5%"
    except Exception as exc:
        return False, str(exc)[:120]


def _check_strategies_have_data(conn) -> tuple[bool, str]:
    """Verify the new strategies are getting at least some data flow.
    Catches bugs like 'firing on empty data'."""
    try:
        ob_recent = conn.execute(
            "SELECT COUNT(*) FROM order_book_snapshots "
            "WHERE ts > extract(epoch FROM NOW() - interval '1 hour')::bigint"
        ).fetchone()[0]
        ticks_recent = conn.execute(
            "SELECT COUNT(*) FROM market_ticks "
            "WHERE timestamp > extract(epoch FROM NOW() - interval '1 hour')::bigint"
        ).fetchone()[0]
        if ob_recent < 50:
            return False, f"only {ob_recent} OB snapshots in last hour (writer broken?)"
        if ticks_recent < 100:
            return False, f"only {ticks_recent} ticks in last hour (collector broken?)"
        return True, f"OB={ob_recent}, ticks={ticks_recent} in last hour"
    except Exception as exc:
        return False, str(exc)[:120]


def _check_wallet_stream_health(conn) -> tuple[bool, str]:
    """Verify watched-wallet trades flowing.

    2026-05-01: WebSocket dropped for 1+ hour while smoke didn't catch.
    2026-05-03: sync_wallet_trades runs every 2h in batches, so the 1h
    window legitimately shows 0 trades in the hour before each sync fires.
    Widened to a 3h window to avoid false positives from the batch cadence
    while still catching genuine stalls (a full 3h gap = missed at least
    one sync cycle).
    """
    try:
        n_3h = conn.execute(
            "SELECT COUNT(*) FROM wallet_trades "
            "WHERE timestamp > extract(epoch FROM NOW() - interval '3 hours')::bigint"
        ).fetchone()[0]
        n_8h = conn.execute(
            "SELECT COUNT(*) FROM wallet_trades "
            "WHERE timestamp > extract(epoch FROM NOW() - interval '8 hours')::bigint"
        ).fetchone()[0]
        # Expected = at least 30% of the per-3h rate implied by the 8h window.
        # A genuine stall (sync container down) shows up as n_3h ≈ 0 for 3+ hours.
        expected = max(10, (n_8h * 3) // (8 * 3))  # 1/3 of 8h total, conservative
        if n_3h < expected:
            return False, (f"wallet stream may be stalled — only {n_3h} "
                           f"trades in last 3h (expected ≥{expected})")
        return True, f"wallet stream healthy ({n_3h} 3h, {n_8h} 8h)"
    except Exception as exc:
        return False, str(exc)[:120]


def _check_paper_position_cap(conn) -> tuple[bool, str]:
    """Verify open-position count isn't blocking new placements.
    2026-05-01: 30 open vs cap=10 silently blocked every signal.
    """
    try:
        from trading_platform.polymarket.execution_gates import _PAPER_THRESHOLDS
        cap = _PAPER_THRESHOLDS.get("max_positions", 10)
        n_open = conn.execute(
            "SELECT COUNT(*) FROM polymarket_paper_trades "
            "WHERE archived = 0 AND exit_ts IS NULL"
        ).fetchone()[0]
        if n_open >= cap * 0.9:
            return False, f"open positions {n_open}/{cap} — near cap, will block new signals"
        return True, f"open positions {n_open}/{cap} (room to spare)"
    except Exception as exc:
        return False, str(exc)[:120]


CHECKS = [
    ("market_ticks_schema", _check_market_ticks_schema),
    ("kalshi_ingest_pace", _check_kalshi_ingest_pace),
    ("calibration_curve_health", _check_calibration_curve_health),
    ("signal_tier_vs_backtest", _check_signal_tier_vs_backtest),
    ("strategies_have_data", _check_strategies_have_data),
    ("wallet_stream_health", _check_wallet_stream_health),
    ("paper_position_cap", _check_paper_position_cap),
]


def run_smoke_tests() -> dict[str, Any]:
    t0 = time.time()
    conn = get_connection()
    results = []
    n_failed = 0
    try:
        for name, fn in CHECKS:
            try:
                ok, detail = fn(conn)
            except Exception as exc:
                ok, detail = False, f"check raised: {exc}"
            results.append({"name": name, "ok": ok, "detail": detail})
            if not ok:
                n_failed += 1
    finally:
        try: conn.close()
        except Exception: pass
    return {
        "elapsed_seconds": round(time.time() - t0, 2),
        "checks_total": len(CHECKS),
        "checks_failed": n_failed,
        "results": results,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    out = run_smoke_tests()
    for r in out["results"]:
        flag = "✓" if r["ok"] else "✗"
        print(f"  [{flag}] {r['name']}: {r['detail']}")
    print(f"\n{out['checks_failed']}/{out['checks_total']} failed in {out['elapsed_seconds']}s")
    return 0 if out["checks_failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
