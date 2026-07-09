"""Regime/drift monitor on the shipped live edges (roadmap C4).

The signal_health IC monitor reads the paper lane (trade_hypotheses) and is
effectively blind to live drift: at C4 recon time it showed n_resolved_30d=0
for resolution_decay and 4 for whale_entry_filtered while live_trades held 15
and 26 resolved rows. This module watches the LIVE lane directly: rolling
per-dollar EV with an event-clustered bootstrap CI band, alerting on state
transitions only. Losing the edge we have costs more than a marginal new one
gains.

States:
  STARVED       n_30d < 5 — not enough live resolutions to say anything
  DRIFT_BREACH  n_30d >= 15 and the entire 30d CI sits below zero — edge gone
  DRIFT_WARN    n_14d >= 8 and 14d EV fell below the 60d lower band — downtrend
  OK            otherwise

Run: python -m trading_platform.polymarket.regime_monitor  (scheduled 6h)
"""
from __future__ import annotations

import logging
import random
import time

logger = logging.getLogger(__name__)

# The two signals with live capital behind them — identical to
# signal_health.PROTECTED_SIGNALS (they can't be auto-deprecated, so this
# monitor is their only live-lane drift coverage).
MONITORED_SIGNALS = ("resolution_decay", "whale_entry_filtered")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS signal_regime (
    signal_type      TEXT PRIMARY KEY,
    n_14d            BIGINT,
    ev_14d           DOUBLE PRECISION,
    n_30d            BIGINT,
    ev_30d           DOUBLE PRECISION,
    ev_lo95_30d      DOUBLE PRECISION,
    ev_hi95_30d      DOUBLE PRECISION,
    n_60d            BIGINT,
    ev_60d           DOUBLE PRECISION,
    ev_lo95_60d      DOUBLE PRECISION,
    ev_hi95_60d      DOUBLE PRECISION,
    n_clusters_30d   BIGINT,
    state            TEXT NOT NULL,
    state_since      BIGINT,
    last_updated     BIGINT NOT NULL
);
"""


def event_clustered_ev_ci(
    rows: list[tuple[float, float, str]], n_iter: int = 1000,
) -> tuple[float, float, float, int, int]:
    """(ev, lo95, hi95, n_trades, n_clusters) from (pnl, stake, cluster) rows.

    Per-dollar EV as ratio-of-sums (sum pnl / sum stake), bootstrapped by
    resampling CLUSTERS with replacement — trades on the same event resolve
    together, so iid trade-level resampling understates the band (governance
    gate G3). Mirrors wallet_behavior_metrics.bootstrap_ci_roi: deterministic
    Random(42), and below 5 clusters no CI is meaningful — wide ±1 band that
    fails any gate.
    """
    clusters: dict[str, list[tuple[float, float]]] = {}
    for pnl, stake, cluster in rows:
        if stake and stake > 0 and pnl is not None:
            clusters.setdefault(cluster or "?", []).append((pnl, stake))
    n_trades = sum(len(v) for v in clusters.values())
    n_clusters = len(clusters)
    if n_trades == 0:
        return (0.0, 0.0, 0.0, 0, 0)
    tot_pnl = sum(p for v in clusters.values() for p, _ in v)
    tot_stake = sum(s for v in clusters.values() for _, s in v)
    ev = tot_pnl / tot_stake if tot_stake > 0 else 0.0
    if n_clusters < 5:
        return (ev, ev - 1.0, ev + 1.0, n_trades, n_clusters)
    keys = list(clusters.keys())
    k = len(keys)
    rng = random.Random(42)
    stats = []
    for _ in range(n_iter):
        sp = ss = 0.0
        for _ in range(k):
            for p, s in clusters[keys[rng.randrange(k)]]:
                sp += p
                ss += s
        stats.append(sp / ss if ss > 0 else 0.0)
    stats.sort()
    lo = stats[int(0.025 * n_iter)]
    hi = stats[int(0.975 * n_iter)]
    return (ev, lo, hi, n_trades, n_clusters)


def classify(n14: int, ev14: float, n30: int, ev30: float,
             lo30: float, hi30: float, n60: int, lo60: float) -> str:
    """Pure state classifier — see module docstring for semantics."""
    if n30 < 5:
        return "STARVED"
    if n30 >= 15 and hi30 < 0:
        return "DRIFT_BREACH"
    if n14 >= 8 and ev14 < lo60:
        return "DRIFT_WARN"
    return "OK"


def _fetch(conn, signal_type: str, days: int, now: int) -> list[tuple]:
    """Resolved live trades windowed on the ECONOMIC date (resolution_date,
    falling back to exit_ts) — booking date windows let back-booked batches
    dump old losses into a rolling window (the 7/2 false breaker trip).
    realized_pnl is from actual fills, already net of execution cost.
    Cluster key: event_slug (with the '-more-markets' suffix normalized off)
    so same-event legs resolve as one bootstrap unit."""
    cutoff = now - days * 86400
    return conn.execute(
        """SELECT lt.realized_pnl, lt.size_usd,
                  COALESCE(REPLACE(m.event_slug, '-more-markets', ''),
                           lt.condition_id) AS cluster
             FROM live_trades lt
             LEFT JOIN markets m ON m.condition_id = lt.condition_id
            WHERE lt.dry_run = 0
              AND lt.signal_type = ?
              AND lt.outcome IN ('win', 'loss')
              AND lt.size_usd > 0
              AND COALESCE(lt.is_probe, 0) = 0
              AND COALESCE(lt.resolution_date, lt.exit_ts) > ?""",
        (signal_type, cutoff),
    ).fetchall()


def run(db_path: str | None = None, now: int | None = None) -> dict:
    """Compute windows, upsert signal_regime, alert on state transitions."""
    from trading_platform.polymarket.db_connection import get_connection

    now = int(now or time.time())
    conn = get_connection(db_path) if db_path else get_connection()
    out: dict[str, dict] = {}
    transitions: list[tuple[str, str, str, dict]] = []
    try:
        conn.execute(_SCHEMA)
        try:
            prev = {
                r[0]: (r[1], r[2]) for r in conn.execute(
                    "SELECT signal_type, state, state_since FROM signal_regime"
                ).fetchall()
            }
        except Exception:
            prev = {}

        for sig in MONITORED_SIGNALS:
            r14 = _fetch(conn, sig, 14, now)
            r30 = _fetch(conn, sig, 30, now)
            r60 = _fetch(conn, sig, 60, now)
            ev14, _, _, n14, _ = event_clustered_ev_ci(r14)
            ev30, lo30, hi30, n30, ncl30 = event_clustered_ev_ci(r30)
            ev60, lo60, hi60, n60, _ = event_clustered_ev_ci(r60)
            state = classify(n14, ev14, n30, ev30, lo30, hi30, n60, lo60)

            prev_state, prev_since = prev.get(sig, (None, None))
            state_since = prev_since if prev_state == state and prev_since else now
            row = {
                "n_14d": n14, "ev_14d": round(ev14, 4),
                "n_30d": n30, "ev_30d": round(ev30, 4),
                "ev_lo95_30d": round(lo30, 4), "ev_hi95_30d": round(hi30, 4),
                "n_60d": n60, "ev_60d": round(ev60, 4),
                "ev_lo95_60d": round(lo60, 4), "ev_hi95_60d": round(hi60, 4),
                "n_clusters_30d": ncl30, "state": state,
            }
            out[sig] = row
            conn.execute(
                """INSERT OR REPLACE INTO signal_regime
                     (signal_type, n_14d, ev_14d, n_30d, ev_30d,
                      ev_lo95_30d, ev_hi95_30d, n_60d, ev_60d,
                      ev_lo95_60d, ev_hi95_60d, n_clusters_30d,
                      state, state_since, last_updated)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (sig, n14, row["ev_14d"], n30, row["ev_30d"],
                 row["ev_lo95_30d"], row["ev_hi95_30d"], n60, row["ev_60d"],
                 row["ev_lo95_60d"], row["ev_hi95_60d"], ncl30,
                 state, state_since, now),
            )
            if prev_state is not None and prev_state != state:
                transitions.append((sig, prev_state, state, row))
            elif prev_state is None and state in ("DRIFT_BREACH", "DRIFT_WARN"):
                # First-ever run landing in a drift state still alerts.
                transitions.append((sig, "(first run)", state, row))
            logger.info(
                "[regime] %s: %s | 14d n=%d ev=%+.3f | 30d n=%d ev=%+.3f "
                "[%+.3f, %+.3f] (%d clusters) | 60d n=%d ev=%+.3f [%+.3f, %+.3f]",
                sig, state, n14, ev14, n30, ev30, lo30, hi30, ncl30,
                n60, ev60, lo60, hi60,
            )
        conn.commit()
    finally:
        try: conn.close()
        except Exception: pass

    if transitions:
        _alert(transitions)
    return out


def _alert(transitions: list[tuple[str, str, str, dict]]) -> None:
    """Telegram on state transitions only (signal_health pattern). BREACH is
    loud; WARN/STARVED/recovery are quiet notifications."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
        from trading_platform.polymarket.telegram_alerts import get_alerter
        alerter = get_alerter()
        if not alerter.enabled:
            return
        loud = any(new == "DRIFT_BREACH" for _, _, new, _ in transitions)
        lines = ["📉 <b>Live-Edge Regime Change</b>" if loud
                 else "📊 <b>Live-Edge Regime Update</b>"]
        for sig, old, new, row in transitions:
            lines.append(
                f"\n<b>{sig}</b>: {old} → <b>{new}</b>\n"
                f"  14d: EV {row['ev_14d']:+.1%} (n={row['n_14d']})\n"
                f"  30d: EV {row['ev_30d']:+.1%} "
                f"[{row['ev_lo95_30d']:+.1%}, {row['ev_hi95_30d']:+.1%}] "
                f"(n={row['n_30d']}, {row['n_clusters_30d']} events)\n"
                f"  60d: EV {row['ev_60d']:+.1%} "
                f"[{row['ev_lo95_60d']:+.1%}, {row['ev_hi95_60d']:+.1%}]"
            )
            if new == "DRIFT_BREACH":
                lines.append("  ⇒ entire 30d CI below zero — review N6 kill "
                             "rule / halve stake cap for this signal")
            elif new == "DRIFT_WARN":
                lines.append("  ⇒ 14d EV below the 60d lower band — watch, "
                             "don't act on one window")
        alerter._send("\n".join(lines), disable_notification=not loud)
    except Exception as exc:
        logger.warning("[regime] Telegram alert failed: %s", exc)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    out = run()
    for sig, row in out.items():
        print(f"{sig}: {row['state']}  30d EV {row['ev_30d']:+.1%} "
              f"[{row['ev_lo95_30d']:+.1%}, {row['ev_hi95_30d']:+.1%}] "
              f"n={row['n_30d']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
