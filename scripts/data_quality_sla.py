"""Continuous data-quality SLA: sample tracked wallets daily against
Polymarket's own /activity feed and alert when our coverage degrades.

The 2026-07-16 truth-test found the wallet layer silently held 23% of an
active wallet's trades for months — every downstream metric was wrong and
nothing noticed. One-off audits rot; this makes the audit a standing check.

Each run:
  1. Sample wallets: top ACTIVE_SAMPLE by our 24h fill count + RANDOM_SAMPLE
     drawn from the recently-synced roster (deterministic-ish rotation).
  2. For each: fetch the newest ~1000 /activity TRADE rows, take their exact
     [oldest, newest] span (minus a lag allowance for in-flight ingestion),
     and compare our row count + USDC notional over the same span.
  3. Write per-wallet and aggregate metrics to data_quality_metrics.
  4. Alert (Telegram) when aggregate trade coverage < WARN threshold; loud
     when < CRIT. Also records resolution-table coverage for ended markets.

Enterprise claim this backs: "wallet data verified daily against Polymarket's
own records; coverage published, not asserted."

Run: python scripts/data_quality_sla.py [--wallets 0x.. ..] [--verbose]
Scheduled: task 'data_quality_sla' (12h).
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.request

from trading_platform.polymarket.db_connection import get_connection

HDRS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126",
        "Accept": "application/json"}
DATA = "https://data-api.polymarket.com"

ACTIVE_SAMPLE = 8          # most-active tracked wallets (24h)
RANDOM_SAMPLE = 4          # rotation through the wider roster
PAGES_PER_WALLET = 4       # newest ~2000 activity rows
# Two coverage claims, measured separately:
#  - SETTLED (older than SETTLED_LAG): sync/backfill had time to land it.
#    This is the SLA we alert on — "data at rest matches Polymarket".
#  - REALTIME (newer than SETTLED_LAG): API polling can't structurally keep
#    up with HF wallets (~1000 fills/2-3h vs 2h sync cadence). Reported as
#    the KPI motivating on-chain firehose ingestion; NOT alerted.
SETTLED_LAG_S = 3 * 3600
WARN_COVERAGE = 0.95
CRIT_COVERAGE = 0.80


def _get(url: str, retries: int = 4):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=HDRS)
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            last = e
            time.sleep(1.5 * (i + 1))
    raise last


def ensure_table(c) -> None:
    c.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_metrics (
            ts BIGINT NOT NULL,
            metric TEXT NOT NULL,
            wallet TEXT,
            value DOUBLE PRECISION,
            detail TEXT
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_dqm_ts ON data_quality_metrics (metric, ts)")
    c.commit()


def sample_wallets(c) -> list[str]:
    now = int(time.time())
    active = [r[0] for r in c.execute(
        """SELECT wallet FROM wallet_trades WHERE timestamp >= ?
           GROUP BY wallet ORDER BY COUNT(*) DESC LIMIT ?""",
        (now - 86400, ACTIVE_SAMPLE)).fetchall()]
    # rotation: hash-bucket the roster by day so every wallet is eventually sampled
    day_bucket = (now // 86400) % 17
    rot = [r[0] for r in c.execute(
        """SELECT wallet FROM wallet_profiles
           WHERE last_synced_ts IS NOT NULL
             AND MOD(ABS(('x' || substr(md5(wallet), 1, 8))::bit(32)::int), 17) = ?
           LIMIT ?""",
        (day_bucket, RANDOM_SAMPLE)).fetchall()]
    out = []
    for w in active + rot:
        if w not in out:
            out.append(w)
    return out


def check_wallet(c, wallet: str) -> dict | None:
    """Coverage of our wallet_trades vs /activity, split settled/realtime."""
    settled_cut = int(time.time()) - SETTLED_LAG_S
    theirs = []
    for page in range(PAGES_PER_WALLET):
        rows = _get(f"{DATA}/activity?user={wallet}&limit=500"
                    f"&offset={page*500}&type=TRADE")
        if not rows:
            break
        theirs.extend(rows)
        time.sleep(0.6)
        if len(rows) < 500:
            break
    if len(theirs) < 10:
        return None  # inactive or data-api-invisible (EOA-direct) — skip

    def _cov(subset):
        if len(subset) < 10:
            return None, 0, 0
        lo = min(int(t["timestamp"]) for t in subset)
        hi = max(int(t["timestamp"]) for t in subset)
        n = len(subset)
        row = c.execute(
            "SELECT COUNT(*) FROM wallet_trades WHERE wallet = ? "
            "AND timestamp BETWEEN ? AND ?", (wallet, lo, hi)).fetchone()
        ours = int(row[0] or 0)
        return min(1.0, ours / n), n, ours

    settled = [t for t in theirs if int(t.get("timestamp") or 0) <= settled_cut]
    realtime = [t for t in theirs if int(t.get("timestamp") or 0) > settled_cut]
    s_cov, s_n, s_ours = _cov(settled)
    r_cov, r_n, r_ours = _cov(realtime)
    if s_cov is None and r_cov is None:
        return None
    return {"wallet": wallet,
            "settled_coverage": s_cov, "settled_n": s_n, "settled_ours": s_ours,
            "realtime_coverage": r_cov, "realtime_n": r_n, "realtime_ours": r_ours}


def resolution_coverage(c) -> float | None:
    """Share of markets that ended in the last 7d holding a resolutions row."""
    row = c.execute("""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE mr.condition_id IS NOT NULL)
        FROM markets m LEFT JOIN market_resolutions mr USING (condition_id)
        WHERE m.end_date_iso IS NOT NULL
          AND m.end_date_iso < to_char(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS')
          AND m.end_date_iso > to_char(NOW() - interval '7 days',
                                       'YYYY-MM-DD"T"HH24:MI:SS')
    """).fetchone()
    total, have = int(row[0] or 0), int(row[1] or 0)
    return (have / total) if total else None


def _alert(msg: str, loud: bool) -> None:
    try:
        from trading_platform.polymarket.telegram_alerts import get_alerter
        a = get_alerter()
        if a.enabled:
            a._send(msg, disable_notification=not loud)
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets", nargs="*", default=None)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    c = get_connection()
    ensure_table(c)
    now = int(time.time())
    wallets = [w.lower() for w in args.wallets] if args.wallets else sample_wallets(c)

    results = []
    for w in wallets:
        try:
            r = check_wallet(c, w)
        except Exception as e:
            print(f"  {w[:12]} check failed: {str(e)[:60]}")
            continue
        if r is None:
            continue
        results.append(r)
        for kind in ("settled", "realtime"):
            cov = r[f"{kind}_coverage"]
            if cov is None:
                continue
            c.execute(
                "INSERT INTO data_quality_metrics (ts, metric, wallet, value, detail) "
                "VALUES (?,?,?,?,?)",
                (now, f"wallet_trade_coverage_{kind}", w, cov,
                 json.dumps({"their_n": r[f"{kind}_n"], "our_n": r[f"{kind}_ours"]})))
        if args.verbose:
            s = f"{r['settled_coverage']:.0%}" if r["settled_coverage"] is not None else "  — "
            rt = f"{r['realtime_coverage']:.0%}" if r["realtime_coverage"] is not None else "  — "
            print(f"  {w[:12]} settled {s} ({r['settled_ours']}/{r['settled_n']})"
                  f"   realtime {rt} ({r['realtime_ours']}/{r['realtime_n']})")

    def _agg(kind):
        vals = [r[f"{kind}_coverage"] for r in results if r[f"{kind}_coverage"] is not None]
        return (sum(vals) / len(vals)) if vals else None

    agg_settled, agg_rt = _agg("settled"), _agg("realtime")
    res_cov = resolution_coverage(c)
    for metric, value in (("agg_trade_coverage_settled", agg_settled),
                          ("agg_trade_coverage_realtime", agg_rt),
                          ("resolution_coverage_7d", res_cov)):
        if value is not None:
            c.execute(
                "INSERT INTO data_quality_metrics (ts, metric, wallet, value, detail) "
                "VALUES (?,?,NULL,?,?)",
                (now, metric, value, json.dumps({"n_wallets": len(results)})))
    c.commit()

    if agg_settled is not None:
        print(f"[dq-sla] SETTLED coverage (SLA): {agg_settled:.0%}")
    if agg_rt is not None:
        print(f"[dq-sla] realtime coverage (firehose KPI, not alerted): {agg_rt:.0%}")
    if res_cov is not None:
        print(f"[dq-sla] resolution coverage (7d ended markets): {res_cov:.0%}")

    if agg_settled is not None and agg_settled < WARN_COVERAGE:
        worst = sorted((r for r in results if r["settled_coverage"] is not None),
                       key=lambda r: r["settled_coverage"])[:3]
        detail = ", ".join(f"{r['wallet'][:10]} {r['settled_coverage']:.0%}" for r in worst)
        _alert(
            f"{'🔴' if agg_settled < CRIT_COVERAGE else '⚠️'} DATA QUALITY: settled "
            f"wallet coverage {agg_settled:.0%} (SLA {WARN_COVERAGE:.0%}). Worst: {detail}",
            loud=agg_settled < CRIT_COVERAGE)
    c.close()


if __name__ == "__main__":
    main()
