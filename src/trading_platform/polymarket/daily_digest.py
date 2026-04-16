"""Daily Telegram digest: live-gate progress toward first live trade."""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from trading_platform.polymarket.db_connection import get_connection

_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _yn_from_direction(direction: str | None) -> str:
    """Convert a stored direction to YES/NO label."""
    d = (direction or "").upper()
    if d in ("BUY", "BUY_YES", "YES"):
        return "YES"
    return "NO"


def build_digest() -> str:
    conn = get_connection()

    def q_one(sql: str, *params) -> int | float | None:
        row = conn.execute(sql, params).fetchone()
        return row[0] if row else None

    live_filter = (
        "signal_type != 'price_velocity' "
        "AND LOWER(category) IN ('politics','geopolitics')"
    )
    resolved = q_one(f"SELECT COUNT(*) FROM signal_outcomes WHERE resolved_at IS NOT NULL AND {live_filter}") or 0
    wins = q_one(f"SELECT COUNT(*) FROM signal_outcomes WHERE is_win=1 AND resolved_at IS NOT NULL AND {live_filter}") or 0
    pending = q_one(f"SELECT COUNT(*) FROM signal_outcomes WHERE resolved_at IS NULL AND {live_filter}") or 0
    ev = q_one(f"SELECT AVG(outcome_delta) FROM signal_outcomes WHERE resolved_at IS NOT NULL AND {live_filter}")

    cutoff = int((datetime.now() - timedelta(hours=24)).timestamp())
    new_sigs = conn.execute(
        "SELECT signal_type, COUNT(*) FROM market_signals "
        "WHERE fired_at > ? AND signal_type != 'price_velocity' "
        "GROUP BY signal_type ORDER BY 2 DESC LIMIT 5",
        (cutoff,),
    ).fetchall()

    # Per-category resolved performance (wallet signals only)
    cat_rows = conn.execute(
        "SELECT LOWER(category), "
        "SUM(CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END) AS res, "
        "SUM(CASE WHEN is_win=1 AND resolved_at IS NOT NULL THEN 1 ELSE 0 END) AS w, "
        "AVG(CASE WHEN resolved_at IS NOT NULL THEN outcome_delta END) AS ev "
        "FROM signal_outcomes "
        "WHERE signal_type != 'price_velocity' AND category IS NOT NULL "
        "GROUP BY LOWER(category) "
        "HAVING res >= 1 ORDER BY res DESC LIMIT 6"
    ).fetchall()

    # Today's wallet-based signals: distribution of YES vs NO direction
    yn_rows = conn.execute(
        "SELECT direction, COUNT(*), AVG(price), "
        "  SUM(CASE WHEN LOWER(category) IN ('politics','geopolitics') THEN 1 ELSE 0 END) "
        "FROM market_signals "
        "WHERE fired_at > ? AND signal_type != 'price_velocity' "
        "GROUP BY direction",
        (cutoff,),
    ).fetchall()

    # Open paper positions summary
    open_rows = conn.execute(
        "SELECT side, COUNT(*), SUM(size_usd), AVG(entry_price), "
        "  SUM(COALESCE(unrealized_pnl,0)) "
        "FROM polymarket_paper_trades "
        "WHERE exit_ts IS NULL AND archived = 0 "
        "GROUP BY side"
    ).fetchall()

    conn.close()

    wr_s = f"{wins/resolved*100:.0f}%" if resolved else "n/a"
    ev_s = f"{ev:+.3f}" if ev is not None else "n/a"
    bar_fill = min(resolved, 20)
    bar = "#" * bar_fill + "." * (20 - bar_fill)

    sig_lines = "\n".join(f"  {s[0]}: {s[1]}" for s in new_sigs) or "  none"

    # Category performance block
    cat_lines = []
    for cat, res, w, cev in cat_rows:
        wr = f"{w/res*100:.0f}%" if res else "n/a"
        evs = f"{cev:+.3f}" if cev is not None else "n/a"
        verdict = "tradeable" if cev and cev > 0 and res >= 20 else "accum"
        cat_lines.append(f"  {cat}: n={res} WR={wr} EV={evs} [{verdict}]")
    cat_block = "\n".join(cat_lines) if cat_lines else "  no resolved data yet"

    # YES/NO direction split of today's wallet signals
    yn_block_parts = []
    for direction, count, avg_p, pg_count in yn_rows:
        yn = _yn_from_direction(direction)
        avg_pct = f"{(avg_p or 0)*100:.0f}%"
        yn_block_parts.append(f"  {yn}: {count} sigs @ avg {avg_pct} implied ({pg_count} in pol/geo)")
    yn_block = "\n".join(yn_block_parts) if yn_block_parts else "  none"

    # Open-position split
    pos_lines = []
    total_deployed = 0.0
    total_unreal = 0.0
    for side, cnt, dep, avg_p, unr in open_rows:
        yn = _yn_from_direction(side)
        total_deployed += (dep or 0)
        total_unreal += (unr or 0)
        pos_lines.append(
            f"  {yn}: {cnt} open, ${dep or 0:,.0f} @ avg {(avg_p or 0)*100:.0f}%, "
            f"unrealized {unr or 0:+.2f}"
        )
    pos_block = "\n".join(pos_lines) if pos_lines else "  no open positions"

    if resolved >= 20 and ev is not None and ev > 0:
        footer = "GATES MET - ready for dry-run"
    else:
        footer = f"Need {max(0, 20 - resolved)} more resolutions"

    return (
        f"Daily Trading Digest - {datetime.now().strftime('%b %d')}\n\n"
        f"Live Gate (politics+geo wallet signals): {resolved}/20 resolved\n"
        f"[{bar}]\n"
        f"WR: {wr_s} | EV: {ev_s}\n"
        f"Pending: {pending} awaiting resolution\n\n"
        f"By category (resolved wallet signals):\n{cat_block}\n\n"
        f"Signals fired 24h (wallet-based):\n{sig_lines}\n\n"
        f"Direction split 24h:\n{yn_block}\n\n"
        f"Open paper positions:\n{pos_block}\n"
        f"  total deployed: ${total_deployed:,.0f} | unrealized: {total_unreal:+.2f}\n\n"
        f"{footer}"
    )


def send_daily_digest() -> bool:
    from dotenv import load_dotenv
    load_dotenv(str(_PROJECT_ROOT / ".env"))
    from trading_platform.polymarket.telegram_alerts import get_alerter

    alerter = get_alerter()
    if not alerter.enabled:
        return False
    # Primary path: use the restructured Postgres-native digest in the
    # alerter (includes paper trades, exits, readiness bars). Falls back
    # to the legacy build_digest() string if something breaks.
    try:
        return alerter.send_signal_digest()
    except Exception:
        pass
    msg = build_digest()
    return bool(alerter._send(msg))


def main() -> int:
    ok = send_daily_digest()
    print(f"digest sent: {ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
