"""Weekly Telegram digest: rolling deep-dive findings + week-over-week deltas.

Runs once a week (Monday 9am via task_scheduler.py). Covers:
  * Gate progress (resolved count, WR, EV in politics+geopolitics)
  * Category performance shift week-over-week
  * Top 3 new wallets to watch
  * Signal-type EV ranking
  * Paper trading PnL delta

Idea: complement daily_digest.py (short operational snapshot) with a
weekly strategic read that informs signal engine / config tuning.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from trading_platform.polymarket.db_connection import get_connection

_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _age_window(days: int) -> tuple[int, int]:
    now = int(datetime.now().timestamp())
    return now - days * 86400, now


def build_digest() -> str:
    conn = get_connection()
    week_start, now = _age_window(7)
    prev_start = week_start - 7 * 86400

    live_filter = (
        "signal_type != 'price_velocity' "
        "AND LOWER(category) IN ('politics','geopolitics')"
    )

    # ── Gate progress ─────────────────────────────────────────────────
    gate = conn.execute(
        f"SELECT "
        f" SUM(CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END), "
        f" SUM(CASE WHEN is_win=1 AND resolved_at IS NOT NULL THEN 1 ELSE 0 END), "
        f" AVG(CASE WHEN resolved_at IS NOT NULL THEN outcome_delta END), "
        f" SUM(CASE WHEN resolved_at IS NULL THEN 1 ELSE 0 END) "
        f"FROM signal_outcomes WHERE {live_filter}"
    ).fetchone()
    resolved, wins, ev, pending = (gate[0] or 0, gate[1] or 0, gate[2], gate[3] or 0)
    wr = f"{wins/resolved*100:.0f}%" if resolved else "n/a"
    ev_s = f"{ev:+.3f}" if ev is not None else "n/a"

    # ── New resolutions this week ─────────────────────────────────────
    newly_resolved = conn.execute(
        f"SELECT COUNT(*) FROM signal_outcomes "
        f"WHERE resolved_at BETWEEN ? AND ? AND {live_filter}",
        (week_start, now),
    ).fetchone()[0] or 0

    # ── Signal fire volume (this week vs previous week) ───────────────
    this_wk = conn.execute(
        "SELECT signal_type, COUNT(*) FROM market_signals "
        "WHERE fired_at BETWEEN ? AND ? AND signal_type != 'price_velocity' "
        "GROUP BY signal_type",
        (week_start, now),
    ).fetchall()
    prev_wk = conn.execute(
        "SELECT signal_type, COUNT(*) FROM market_signals "
        "WHERE fired_at BETWEEN ? AND ? AND signal_type != 'price_velocity' "
        "GROUP BY signal_type",
        (prev_start, week_start),
    ).fetchall()
    this_map = dict(this_wk)
    prev_map = dict(prev_wk)
    all_types = sorted(set(this_map) | set(prev_map))
    sig_lines = []
    for st in all_types:
        t, p = this_map.get(st, 0), prev_map.get(st, 0)
        delta = t - p
        arrow = "↑" if delta > 0 else "↓" if delta < 0 else "="
        sig_lines.append(f"  {st}: {t} (wk-1: {p}, {arrow}{abs(delta)})")

    # ── Category EV (resolved signals, any category) ──────────────────
    cats = conn.execute(
        "SELECT LOWER(category), "
        " SUM(CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END), "
        " SUM(CASE WHEN is_win=1 AND resolved_at IS NOT NULL THEN 1 ELSE 0 END), "
        " AVG(CASE WHEN resolved_at IS NOT NULL THEN outcome_delta END) "
        "FROM signal_outcomes "
        "WHERE signal_type != 'price_velocity' AND category IS NOT NULL "
        "GROUP BY LOWER(category) "
        "HAVING SUM(CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END) >= 5 "
        "ORDER BY 4 DESC"
    ).fetchall()
    cat_lines = []
    for cat, res, w, cev in cats:
        wr_s = f"{w/res*100:.0f}%" if res else "n/a"
        evs = f"{cev:+.3f}" if cev is not None else "n/a"
        cat_lines.append(f"  {cat}: n={res} WR={wr_s} EV={evs}")

    # ── Paper trading week performance ─────────────────────────────────
    paper = conn.execute(
        "SELECT "
        " COUNT(*), "
        " SUM(CASE WHEN exit_ts IS NULL THEN 1 ELSE 0 END), "
        " SUM(CASE WHEN exit_ts BETWEEN ? AND ? THEN 1 ELSE 0 END), "
        " SUM(CASE WHEN exit_ts BETWEEN ? AND ? AND realized_pnl>0 THEN 1 ELSE 0 END), "
        " SUM(CASE WHEN exit_ts BETWEEN ? AND ? THEN realized_pnl ELSE 0 END) "
        "FROM polymarket_paper_trades",
        (week_start, now, week_start, now, week_start, now),
    ).fetchone()
    total_paper = paper[0] or 0
    open_paper = paper[1] or 0
    closed_wk = paper[2] or 0
    won_wk = paper[3] or 0
    pnl_wk = paper[4] or 0
    paper_wr = f"{won_wk/closed_wk*100:.0f}%" if closed_wk else "n/a"

    # ── Top 3 wallets by recent net PnL ────────────────────────────────
    top_wallets = conn.execute(
        "SELECT wallet, net_pnl_usdc, directional_win_rate, resolved_trades "
        "FROM wallet_profiles "
        "WHERE net_pnl_usdc > 0 AND resolved_trades >= 20 "
        "ORDER BY net_pnl_usdc DESC LIMIT 3"
    ).fetchall()
    wallet_lines = [
        f"  {w[:14]} ${pnl:,.0f} WR={dwr:.0%} n={n}"
        for w, pnl, dwr, n in top_wallets
    ]

    conn.close()

    # ── Assemble ───────────────────────────────────────────────────────
    bar_fill = min(resolved, 20)
    bar = "#" * bar_fill + "." * (20 - bar_fill)
    verdict = (
        "GATES MET - ready for dry-run"
        if resolved >= 20 and ev is not None and ev > 0
        else f"Need {max(0, 20 - resolved)} more resolutions"
    )

    return "\n".join([
        f"Weekly Trading Digest - {datetime.now().strftime('%b %d')}",
        "",
        f"LIVE GATE (politics+geopolitics, non-velocity):",
        f"  [{bar}]  {resolved}/20 resolved",
        f"  WR: {wr} | EV: {ev_s} | pending: {pending}",
        f"  +{newly_resolved} resolved this week",
        "",
        "CATEGORY EV (all resolved wallet signals):",
        *(cat_lines or ["  no resolved data yet"]),
        "",
        "SIGNAL VOLUME (this wk vs prev wk):",
        *(sig_lines or ["  no wallet signals"]),
        "",
        "PAPER TRADING (this week):",
        f"  closed: {closed_wk} (WR {paper_wr}, PnL {pnl_wk:+.2f})",
        f"  open: {open_paper} positions",
        "",
        "TOP WALLETS (by net PnL):",
        *(wallet_lines or ["  n/a"]),
        "",
        verdict,
    ])


def send_weekly_digest() -> bool:
    from dotenv import load_dotenv
    load_dotenv(str(_PROJECT_ROOT / ".env"))
    from trading_platform.polymarket.telegram_alerts import get_alerter

    alerter = get_alerter()
    if not alerter.enabled:
        return False
    msg = build_digest()
    # Prefer pipeline_alert channel
    for method in ("send_pipeline_alert", "send_daily_update", "_send", "send"):
        fn = getattr(alerter, method, None)
        if not callable(fn):
            continue
        try:
            if method == "send_pipeline_alert":
                return bool(fn("weekly_digest", msg, "info"))
            return bool(fn(msg))
        except Exception:
            continue
    return False


def main() -> int:
    ok = send_weekly_digest()
    print(f"weekly digest sent: {ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
