"""
Health watchdog for the trading platform.

Polls the API, scheduler state, and SQLite database every 5 minutes.
On any failure, fires a Telegram alert with specific component
status. Tracks each component's first-failure timestamp in memory so
we don't spam the same alert every cycle — alert once on degradation,
once on recovery.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import urllib.request
import urllib.error

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

try:
    from trading_platform.polymarket.logging_config import setup_logging
    setup_logging(service="watchdog")
except Exception:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("watchdog")

API_URL = os.environ.get("WATCHDOG_API_URL", "http://api:8001")
SCHEDULER_STATE = PROJECT_ROOT / "data" / "scheduler" / "state.json"
DB_PATH = PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db"
POLL_INTERVAL_SECONDS = 5 * 60
FREE_DISK_WARN_GB = 5.0
# Dampen: require N consecutive failures before alerting. Stops spurious
# alerts from single-cycle blips (the 03:33 + 04:01 + 16:02 API alerts
# were all 1-cycle false positives during container startup).
CONSECUTIVE_FAILURES_TO_ALERT = 2
# Scheduler tasks are flagged stale only if overdue by more than this
# multiple of their interval. 2x was too tight — a 5-min poller that
# takes 140s on a crowded cycle would trip the alert.
SCHEDULER_STALE_MULTIPLIER = 3
# Grace period after watchdog start: don't alert until this many seconds
# have elapsed. Prevents the classic "API not up yet" alert when the whole
# stack starts at once. Covers FastAPI boot + DB ready + initial healthcheck.
STARTUP_GRACE_SECONDS = 120

# N1 dead-man's-switch SLA thresholds.
BALANCE_SNAPSHOT_STALE_S = int(os.environ.get("WATCHDOG_BALANCE_STALE_S", str(2 * 3600)))
# 2026-07-08: 15m → 45m. Whale ingestion is bursty (overnight lulls); the
# tight threshold produced 12 false poller_freshness alerts/day on a 2h
# metronome while ingestion was actually healthy (8,993 rows/24h).
POLLER_STALE_S = int(os.environ.get("WATCHDOG_POLLER_STALE_S", str(45 * 60)))
RECONCILE_MAX_AGE_S = int(os.environ.get("WATCHDOG_RECONCILE_MAX_AGE_S", str(26 * 3600)))
# Trading-liveness SLA. Every OTHER check here is infrastructure-liveness; the
# stack can be 100% green while the money loop is dead (the 2026-07-13→16 case:
# a silent auto-demote blocked every entry for ~3 DAYS and no check noticed).
# This is the missing business-function heartbeat: alert when NO live fill has
# landed in this many hours WHILE the engine is still actively generating
# (blocked) signals — i.e. it WANTS to trade but something gates every attempt.
# The "engine still trying" guard keeps a genuinely quiet market (no candidates)
# from paging. 18h clears normal overnight lulls (real inter-fill gaps run ~8h).
TRADING_LIVENESS_MAX_S = int(os.environ.get("WATCHDOG_TRADING_LIVENESS_S", str(18 * 3600)))
# Components severe enough to auto-HALT live trading (write KILL_SWITCH_ACTIVE)
# on sustained failure. Only balance_staleness: a frozen/stale balance API
# means our equity and therefore our sizing is flat-out wrong — new entries
# must stop. reconcile drift is a loud ALERT (investigate) but NOT an auto-halt:
# the reconciler runs only every 4h, so a transient drift would wrongly halt
# for hours, and real losses are already covered by the kill-switch breakers.
_SEVERE_HALT_COMPONENTS = {"balance_staleness"}
# Auto-halt requires MORE sustained failure than a plain alert, so a single
# blip never trips the kill switch.
HALT_CONSECUTIVE_FAILURES = int(os.environ.get("WATCHDOG_HALT_FAILURES", "4"))
# Daily "still alive" ping cadence — silence then means the watchdog is dead.
ALIVE_PING_INTERVAL_S = int(os.environ.get("WATCHDOG_ALIVE_PING_S", str(24 * 3600)))


@dataclass
class ComponentState:
    name: str
    healthy: bool
    detail: str
    failed_since: float | None = None


def _http_get(url: str, timeout: float = 5.0) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status, resp.read(2048).decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        return e.code, str(e)
    except Exception as e:
        return 0, str(e)


def check_api() -> ComponentState:
    code, body = _http_get(f"{API_URL}/api/system/status")
    return ComponentState(
        name="api",
        healthy=(code == 200),
        detail=f"HTTP {code}" if code else body[:80],
    )


def check_db() -> ComponentState:
    """Check the canonical Postgres DB (the SQLite file is stale post-cutover).
    Falls back to the SQLite health check only if psycopg isn't installed.
    """
    backend = os.environ.get("DB_BACKEND", "postgres").lower()
    if backend == "postgres":
        try:
            import psycopg
            host = os.environ.get("POSTGRES_HOST", "postgres")
            port = int(os.environ.get("POSTGRES_PORT", "5432"))
            user = os.environ.get("POSTGRES_USER", "polymarket")
            pwd = os.environ.get("POSTGRES_PASSWORD", "polymarket_dev")
            dbn = os.environ.get("POSTGRES_DB", "polymarket")
            with psycopg.connect(host=host, port=port, user=user,
                                 password=pwd, dbname=dbn, connect_timeout=3) as conn:
                cur = conn.execute("SELECT COUNT(*) FROM wallet_trades")
                n = cur.fetchone()[0]
            return ComponentState("db", True, f"{n} wallet_trades (pg)")
        except Exception as e:
            return ComponentState("db", False, f"pg: {str(e)[:80]}")

    if not DB_PATH.exists():
        return ComponentState("db", False, f"missing: {DB_PATH}")
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=2.0)
        try:
            n = conn.execute("SELECT COUNT(*) FROM wallet_trades").fetchone()[0]
        finally:
            conn.close()
        return ComponentState("db", True, f"{n} wallet_trades (sqlite)")
    except Exception as e:
        return ComponentState("db", False, str(e)[:80])


def check_scheduler() -> ComponentState:
    if not SCHEDULER_STATE.exists():
        return ComponentState("scheduler", False, "no state file")
    try:
        data = json.loads(SCHEDULER_STATE.read_text(encoding="utf-8"))
    except Exception as e:
        return ComponentState("scheduler", False, f"bad state: {e}")
    now = time.time()
    stale: list[str] = []
    for t in data.get("tasks", []):
        if not t.get("enabled", True):
            continue
        last_run = t.get("last_run_at")
        interval = t.get("interval_seconds", 0)
        if last_run is None:
            continue  # not yet run, OK on first cycle
        # Stale if more than 2x its interval overdue
        if interval and now - float(last_run) > SCHEDULER_STALE_MULTIPLIER * interval:
            stale.append(t["name"])
    if stale:
        return ComponentState(
            "scheduler", False, f"stale tasks: {', '.join(stale[:5])}",
        )
    return ComponentState("scheduler", True, f"{len(data.get('tasks', []))} tasks tracked")


def _check_heartbeat(service_key: str, stale_s: int = 180) -> ComponentState:
    """service_health-row freshness AND status check.

    Freshness alone is not enough: wallet_stream wrote a fresh 'ok' row
    every 60s for a WEEK while delivering zero events (2026-06-30 → 07-07).
    Services now self-report 'degraded' when alive-but-not-working; treat
    that as unhealthy so it alerts.
    """
    if os.environ.get("DB_BACKEND", "postgres").lower() != "postgres":
        return ComponentState(service_key, True, "skipped (sqlite backend)")
    try:
        import psycopg
        host = os.environ.get("POSTGRES_HOST", "postgres")
        port = int(os.environ.get("POSTGRES_PORT", "5432"))
        user = os.environ.get("POSTGRES_USER", "polymarket")
        pwd = os.environ.get("POSTGRES_PASSWORD", "polymarket_dev")
        dbn = os.environ.get("POSTGRES_DB", "polymarket")
        with psycopg.connect(host=host, port=port, user=user,
                             password=pwd, dbname=dbn, connect_timeout=3) as conn:
            cur = conn.execute(
                "SELECT checked_at, status, error_message FROM service_health "
                "WHERE service = %s ORDER BY checked_at DESC LIMIT 1",
                (service_key,),
            )
            row = cur.fetchone()
        if not row or not row[0]:
            return ComponentState(service_key, False, "no heartbeat yet")
        last, status, detail = int(row[0]), (row[1] or "").lower(), row[2] or ""
        age = time.time() - last
        if age > stale_s:
            return ComponentState(service_key, False, f"last heartbeat {age:.0f}s ago (threshold {stale_s}s)")
        if status and status != "ok":
            return ComponentState(
                service_key, False,
                f"heartbeat fresh but status={status}: {detail[:160]}")
        return ComponentState(service_key, True, f"heartbeat {age:.0f}s ago")
    except Exception as e:
        return ComponentState(service_key, False, f"heartbeat check err: {str(e)[:80]}")


def check_wallet_stream() -> ComponentState:
    """Polygon WS listener heartbeat. Stale >3 min = likely disconnected."""
    return _check_heartbeat("wallet_stream", stale_s=180)


def check_critical_loops() -> ComponentState:
    """Money-loop runner heartbeat. As of 2026-07-23 the money-critical loops
    (live_equity_snapshot / live_position_monitor / maker_experiment) run in a
    SEPARATE container (scripts/critical_loops.py), extracted from the scheduler
    after it went dispatch-dead 4x in 5 days and took the money loop down with
    it each time. That container writes a 'critical_loops' service_health row
    every ~60s and self-reports status='degraded' when a loop stops COMPLETING
    (fresh heartbeat but stale last-success — the wallet_stream lesson). This
    check catches the runner being dead/wedged; balance_staleness +
    trading_liveness verify the OUTCOMES (fresh equity snapshot, live fills)."""
    return _check_heartbeat("critical_loops", stale_s=180)


def check_live_collect() -> ComponentState:
    """Detect silent WebSocket death via service_health heartbeat.

    live_collector writes ("live_collect", "ok") every cycle (~5s). We
    flag the service stale if the most recent heartbeat is >3 min old —
    a real WS stall would otherwise go unnoticed until wallet_trades
    freshness alerts fire much later.
    """
    STALE_THRESHOLD_S = 180
    backend = os.environ.get("DB_BACKEND", "postgres").lower()
    if backend != "postgres":
        return ComponentState("live_collect", True, "skipped (sqlite backend)")
    try:
        import psycopg
        host = os.environ.get("POSTGRES_HOST", "postgres")
        port = int(os.environ.get("POSTGRES_PORT", "5432"))
        user = os.environ.get("POSTGRES_USER", "polymarket")
        pwd = os.environ.get("POSTGRES_PASSWORD", "polymarket_dev")
        dbn = os.environ.get("POSTGRES_DB", "polymarket")
        with psycopg.connect(host=host, port=port, user=user,
                             password=pwd, dbname=dbn, connect_timeout=3) as conn:
            cur = conn.execute(
                "SELECT MAX(checked_at) FROM service_health WHERE service = 'live_collect'"
            )
            row = cur.fetchone()
        last = int(row[0]) if row and row[0] else 0
        age = time.time() - last
        if not last:
            return ComponentState("live_collect", False, "no heartbeat yet")
        if age > STALE_THRESHOLD_S:
            return ComponentState(
                "live_collect", False, f"last heartbeat {age:.0f}s ago (threshold {STALE_THRESHOLD_S}s)",
            )
        return ComponentState("live_collect", True, f"heartbeat {age:.0f}s ago")
    except Exception as e:
        return ComponentState("live_collect", False, f"heartbeat check err: {str(e)[:80]}")


def check_disk() -> ComponentState:
    try:
        free_bytes = shutil.disk_usage(str(PROJECT_ROOT)).free
        free_gb = free_bytes / (1024 ** 3)
        if free_gb < FREE_DISK_WARN_GB:
            return ComponentState("disk", False, f"only {free_gb:.1f}GB free")
        return ComponentState("disk", True, f"{free_gb:.0f}GB free")
    except Exception as e:
        return ComponentState("disk", False, str(e)[:80])


def check_hypothesis_drift() -> ComponentState:
    """Alert if closed paper trades have matching hypothesis rows that
    were never resolved. Before 2026-04-18 this drift went silent for
    weeks — 17 resolved trades had zero updated hypotheses, and the
    thesis scorecard was stuck reporting 0% accuracy despite real
    outcomes being recorded in polymarket_paper_trades.
    """
    if os.environ.get("DB_BACKEND", "postgres").lower() != "postgres":
        return ComponentState("hypothesis_drift", True, "skipped (sqlite)")
    try:
        import psycopg
        conninfo = (
            f"host={os.environ.get('POSTGRES_HOST','postgres')} "
            f"port={os.environ.get('POSTGRES_PORT','5432')} "
            f"user={os.environ.get('POSTGRES_USER','polymarket')} "
            f"password={os.environ.get('POSTGRES_PASSWORD','polymarket_dev')} "
            f"dbname={os.environ.get('POSTGRES_DB','polymarket')}"
        )
        with psycopg.connect(conninfo, connect_timeout=10) as conn:
            row = conn.execute(
                """SELECT COUNT(*) FROM trade_hypotheses h
                     JOIN polymarket_paper_trades pt ON pt.id = h.trade_id
                    WHERE h.actual_outcome IS NULL
                      AND pt.exit_ts IS NOT NULL
                      AND pt.outcome IS NOT NULL
                      AND pt.exit_ts < extract(epoch FROM NOW()) - 3600"""
            ).fetchone()
            drift = int(row[0]) if row else 0
        if drift > 0:
            return ComponentState(
                "hypothesis_drift", False,
                f"{drift} closed trades >1h old with unresolved hypothesis "
                "(run mark_resolved backfill)"
            )
        return ComponentState("hypothesis_drift", True, "scorecard fresh")
    except Exception as e:
        return ComponentState("hypothesis_drift", False, f"check err: {str(e)[:80]}")


# External dead-man's switch. The in-stack watchdog cannot alert when the
# HOST is off — it dies with the host. On 2026-06-10→15 the host PC was
# powered off for ~5.7 days while unattended and nothing surfaced it; the
# whole stack (watchdog + Telegram included) was down. The only way to catch
# host-down is an OUTBOUND ping to a service OFF this host: each cycle we ping
# HEARTBEAT_PING_URL; if those pings stop arriving, that external monitor
# (e.g. a free healthchecks.io check with period 5m + grace) alerts the
# operator. Set HEARTBEAT_PING_URL to the check's ping URL to enable; unset =
# no-op (preserves current behaviour).
HEARTBEAT_PING_URL = os.environ.get("HEARTBEAT_PING_URL", "").strip()
# Components whose failure should escalate the external monitor to a paging
# state (ping the /fail endpoint) rather than a plain liveness ping. Soft
# checks (disk, hypothesis_drift) are excluded — they shouldn't page the phone.
_HEARTBEAT_HARD_COMPONENTS = {"api", "db", "scheduler", "critical_loops", "live_collect"}


def _external_heartbeat(states: list[ComponentState]) -> None:
    """Ping an external monitor so host-down is detectable off-host.

    Sends a liveness ping each cycle. If any hard component is unhealthy and
    the ping URL supports it (healthchecks.io-style), append ``/fail`` so the
    external monitor escalates immediately instead of waiting for the grace
    window to lapse. Best-effort: never raises, never blocks the loop.
    """
    if not HEARTBEAT_PING_URL:
        return
    hard_down = any(
        (not s.healthy) and s.name in _HEARTBEAT_HARD_COMPONENTS for s in states
    )
    url = HEARTBEAT_PING_URL.rstrip("/") + "/fail" if hard_down else HEARTBEAT_PING_URL
    try:
        with urllib.request.urlopen(url, timeout=8.0) as resp:
            resp.read(64)
    except Exception as exc:  # noqa: BLE001 - heartbeat must never crash the loop
        logger.debug("[heartbeat] external ping failed: %s", exc)


def _send_alert(message: str, *, loud: bool = False) -> None:
    try:
        from trading_platform.polymarket.telegram_alerts import get_alerter
        alerter = get_alerter()
        if alerter.enabled:
            alerter._send(message, disable_notification=not loud)
    except Exception:
        pass


def format_status(states: list[ComponentState]) -> str:
    icons = {True: "\u2705", False: "\u274c"}
    lines = []
    for s in states:
        lines.append(f"{icons[s.healthy]} {s.name}: {s.detail}")
    return "\n".join(lines)


def check_scheduler_consecutive_failures() -> ComponentState:
    """Read state.json and alert when any enabled task has 5+ consecutive
    failures. Wired 2026-04-24 — was the missing tripwire during the
    week-long unattended run when 6 paper-side tasks failed every 15min
    for 5 days without surfacing as a single component-level alert.
    """
    if not SCHEDULER_STATE.exists():
        return ComponentState("scheduler_failures", True, "no state file (skipped)")
    try:
        data = json.loads(SCHEDULER_STATE.read_text(encoding="utf-8"))
    except Exception as exc:
        return ComponentState("scheduler_failures", False, f"bad state: {exc}")
    bad = []
    for t in data.get("tasks", []):
        if not t.get("enabled", True):
            continue
        cf = int(t.get("consecutive_failures", 0) or 0)
        if cf >= 5:
            bad.append(f"{t['name']}={cf}x")
    if bad:
        return ComponentState(
            "scheduler_failures", False,
            f"{len(bad)} task(s) failing 5+x: {', '.join(bad[:5])}",
        )
    return ComponentState("scheduler_failures", True, "no sustained failures")


def _watchdog_pg():
    """Open a short-lived psycopg connection using the watchdog's env, or
    None if the sqlite backend is active or psycopg is unavailable."""
    if os.environ.get("DB_BACKEND", "postgres").lower() != "postgres":
        return None
    import psycopg
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "polymarket"),
        password=os.environ.get("POSTGRES_PASSWORD", "polymarket_dev"),
        dbname=os.environ.get("POSTGRES_DB", "polymarket"),
        connect_timeout=3,
    )


def check_balance_staleness() -> ComponentState:
    """The equity/balance view must be fresh AND moving. A frozen usdc_balance
    across many snapshots is the documented stale-balance-API failure (froze at
    $5.43 for 20 runs). Either mode makes our equity/sizing untrustworthy."""
    name = "balance_staleness"
    try:
        conn = _watchdog_pg()
        if conn is None:
            return ComponentState(name, True, "skipped (sqlite backend)")
        # psycopg3 CLOSES the connection when a `with conn:` block exits
        # (unlike psycopg2, which only ended the transaction). A second
        # `with conn:` here therefore raised "the connection is closed" on
        # every cycle the frozen branch ran — which is ALWAYS during a
        # halt — making the check permanently unhealthy and the AUTO-CLEAR
        # unreachable. The 7/8 fix re-created the very halt-lock loop it
        # was written to break. BOTH queries now share one context.
        with conn:
            rows = conn.execute(
                "SELECT ts, usdc_balance FROM live_equity_snapshots "
                "ORDER BY ts DESC LIMIT 8"
            ).fetchall()
            moved_n = None
            if rows:
                window_lo = int(rows[-1][0])
                moved = conn.execute(
                    "SELECT COUNT(*) FROM live_trades WHERE dry_run = 0 "
                    "AND (filled_at >= %s OR exit_ts >= %s)",
                    (window_lo, window_lo),
                ).fetchone()
                moved_n = int(moved[0] or 0)
        if not rows:
            return ComponentState(name, False, "no equity snapshots")
        age = time.time() - int(rows[0][0])
        if age > BALANCE_SNAPSHOT_STALE_S:
            return ComponentState(name, False,
                                  f"no fresh equity snapshot in {age/60:.0f}m")
        vals = [float(r[1]) for r in rows if r[1] is not None]
        if len(vals) >= 6 and max(vals) - min(vals) < 1e-9:
            span_h = (int(rows[0][0]) - int(rows[-1][0])) / 3600
            # Frozen balance is only suspicious if money MOVED in the
            # window. During a halt (or a quiet stretch) an unchanged USDC
            # is expected.
            if (moved_n or 0) == 0:
                return ComponentState(
                    name, True,
                    f"fresh ({age/60:.0f}m); usdc unchanged ${vals[0]:.2f} "
                    f"but no fills/exits in {span_h:.1f}h — expected")
            return ComponentState(
                name, False,
                f"usdc_balance frozen at ${vals[0]:.2f} across {len(vals)} "
                f"snapshots (~{span_h:.1f}h) DESPITE {moved_n} "
                f"fills/exits — balance API likely stale")
        return ComponentState(name, True, f"fresh ({age/60:.0f}m), moving")
    except Exception as e:
        return ComponentState(name, False, f"check err: {str(e)[:80]}")


def check_poller_freshness() -> ComponentState:
    """Wallet-trade ingestion must be recent — the poller writes synced_at on
    every ingested trade. Stale ingestion means the copy/whale lanes are blind."""
    name = "poller_freshness"
    try:
        conn = _watchdog_pg()
        if conn is None:
            return ComponentState(name, True, "skipped (sqlite backend)")
        with conn:
            row = conn.execute("SELECT MAX(synced_at) FROM wallet_trades").fetchone()
        last = int(row[0]) if row and row[0] else 0
        if not last:
            return ComponentState(name, False, "no wallet_trades ingested")
        age = time.time() - last
        if age > POLLER_STALE_S:
            return ComponentState(name, False,
                                  f"no wallet-trade ingestion in {age/60:.0f}m")
        return ComponentState(name, True, f"ingested {age/60:.0f}m ago")
    except Exception as e:
        return ComponentState(name, False, f"check err: {str(e)[:80]}")


def check_trading_liveness() -> ComponentState:
    """Business-function heartbeat: the live lane must actually be FILLING, not
    just be infrastructurally alive. Fires when no real fill has landed in
    TRADING_LIVENESS_MAX_S while the signal engine is still generating attempts
    that get blocked — the exact silent-dark-lane failure (auto-demote, stuck
    gate, mis-config) that every other check is blind to. Names the top block
    reason so the operator sees WHY, not just THAT, the lane went dark."""
    name = "trading_liveness"
    try:
        conn = _watchdog_pg()
        if conn is None:
            return ComponentState(name, True, "skipped (sqlite backend)")
        with conn:
            row = conn.execute(
                "SELECT MAX(GREATEST(COALESCE(filled_at,0), COALESCE(attempted_at,0))) "
                "FROM live_trades WHERE dry_run = 0 AND status = 'matched'"
            ).fetchone()
            last_fill = int(row[0]) if row and row[0] else 0
            # Is the engine still actively trying? Count blocked live-intent
            # attempts in the last 2h; if zero, the market is genuinely quiet
            # (nothing to trade) and a dark lane is expected — don't page.
            since_2h = int(time.time()) - 2 * 3600
            brow = conn.execute(
                "SELECT COUNT(*), MODE() WITHIN GROUP (ORDER BY error_msg) "
                "FROM live_trades WHERE status = 'blocked' AND attempted_at >= %s",
                (since_2h,),
            ).fetchone()
            blocked_2h = int(brow[0]) if brow and brow[0] else 0
            top_reason = (brow[1] if brow and brow[1] else "") or ""
        if not last_fill:
            # No live fill ever — only meaningful if the engine is trying.
            if blocked_2h > 0:
                return ComponentState(name, False,
                                      f"no live fill EVER; {blocked_2h} attempts "
                                      f"blocked in 2h — top: {top_reason[:80]}")
            return ComponentState(name, True, "no live fills yet (engine idle)")
        age = time.time() - last_fill
        if age > TRADING_LIVENESS_MAX_S and blocked_2h > 0:
            return ComponentState(
                name, False,
                f"DARK LANE: no live fill in {age/3600:.0f}h despite {blocked_2h} "
                f"blocked attempts in 2h — top: {top_reason[:80]}")
        if age > TRADING_LIVENESS_MAX_S:
            # Dark but engine not generating — quiet market, informational only.
            return ComponentState(name, True,
                                  f"no fill in {age/3600:.0f}h (engine idle — no "
                                  f"blocked attempts in 2h; quiet market)")
        return ComponentState(name, True, f"last fill {age/3600:.1f}h ago")
    except Exception as e:
        return ComponentState(name, False, f"check err: {str(e)[:80]}")


def check_firehose() -> ComponentState:
    """Chain-firehose liveness: distinct wallets ingested in the last 15min.

    Catches every silent-degradation mode seen on 2026-07-20 in one signal:
    persist dead (DB conn rot: 4.5h outage, hundreds of wallets -> 0),
    narrow-mode reversion (.env regenerated without WS_FORCE_BROAD: ~2000 ->
    <50 wallets), WS provider dark, or CHAIN_FIREHOSE_PERSIST flipped off.
    Threshold 100 sits far below broad-mode reality (~2000+/15min) and above
    narrow mode's tracked-only trickle."""
    name = "firehose"
    try:
        conn = _watchdog_pg()
        if conn is None:
            return ComponentState(name, True, "skipped (sqlite backend)")
        with conn:
            row = conn.execute(
                "SELECT COUNT(DISTINCT wallet), COUNT(*) FROM wallet_trades "
                "WHERE synced_at >= %s", (int(time.time()) - 900,)
            ).fetchone()
        wallets, rows = int(row[0] or 0), int(row[1] or 0)
        threshold = int(os.environ.get("WATCHDOG_FIREHOSE_MIN_WALLETS", "100"))
        if wallets < threshold:
            return ComponentState(
                name, False,
                f"only {wallets} distinct wallets ingested in 15m "
                f"(threshold {threshold}) — firehose degraded/narrow/dead")
        return ComponentState(name, True, f"{wallets} wallets / {rows} rows in 15m")
    except Exception as e:
        return ComponentState(name, False, f"check err: {str(e)[:80]}")


def check_reconcile_age() -> ComponentState:
    """Days since reconcile_polymarket_truth last ran CLEAN (exit 0 = zero
    drifts). A long gap means our ledger may silently diverge from on-chain."""
    name = "reconcile_age"
    if not SCHEDULER_STATE.exists():
        return ComponentState(name, True, "no state file (skipped)")
    try:
        data = json.loads(SCHEDULER_STATE.read_text(encoding="utf-8"))
    except Exception as exc:
        return ComponentState(name, False, f"bad state: {exc}")
    task = next((t for t in data.get("tasks", [])
                 if t.get("name") == "reconcile_polymarket_truth"), None)
    if not task:
        return ComponentState(name, True, "reconcile task not tracked")
    last_run = task.get("last_run_at")
    status = task.get("last_status")
    if last_run is None:
        return ComponentState(name, False, "reconcile has never run")
    age = time.time() - float(last_run)
    # 'failed' = the reconciler exits 2 on drift; that's an outstanding drift.
    if status == "failed":
        return ComponentState(name, False,
                              f"reconcile drift OUTSTANDING (last run {age/3600:.0f}h ago)")
    if age > RECONCILE_MAX_AGE_S:
        return ComponentState(name, False,
                              f"no clean reconcile in {age/3600:.0f}h")
    return ComponentState(name, True, f"clean {age/3600:.0f}h ago")


# Auto-clear: consecutive HEALTHY balance cycles required before a
# watchdog-tripped balance_staleness halt un-trips itself. At the 60s poll
# this is ~30 min of sustained recovery.
AUTO_CLEAR_CONSECUTIVE = int(os.environ.get("WATCHDOG_AUTO_CLEAR_CYCLES", "30"))

# ── Scheduler auto-remediation (2026-07-22) ─────────────────────────────────
# Fourth silent-scheduler incident in 5 days: process alive, zero dispatch,
# zero logs — the in-process stall self-heal can't catch pre-main/log-dead
# wedges, and the watchdog's alert-once design sent ONE Telegram at 01:21
# then nothing for 23 hours. Detection without remediation. The watchdog now
# RESTARTS the scheduler container via the docker socket when the scheduler
# check fails this many consecutive cycles (3 x 300s = 15 min), rate-limited
# to one remediation per 2h so a genuinely broken scheduler can't flap.
SCHED_REMEDIATE_FAILURES = int(os.environ.get("WATCHDOG_SCHED_REMEDIATE_FAILURES", "3"))
SCHED_REMEDIATE_COOLDOWN_S = int(os.environ.get("WATCHDOG_SCHED_REMEDIATE_COOLDOWN_S", str(2 * 3600)))
DOCKER_SOCK = "/var/run/docker.sock"
# Re-alert cadence for components that STAY failing (was: alert once, forever
# silent — trivially missable).
REALERT_INTERVAL_S = int(os.environ.get("WATCHDOG_REALERT_S", str(6 * 3600)))


def _docker_restart(container: str, timeout_s: int = 10) -> bool:
    """Restart a container via the docker socket (stdlib only)."""
    import socket as _s
    try:
        sock = _s.socket(_s.AF_UNIX, _s.SOCK_STREAM)
        sock.settimeout(timeout_s + 20)
        sock.connect(DOCKER_SOCK)
        req = (f"POST /containers/{container}/restart?t={timeout_s} HTTP/1.1\r\n"
               f"Host: docker\r\nContent-Length: 0\r\n\r\n")
        sock.sendall(req.encode())
        resp = sock.recv(4096).decode(errors="replace")
        sock.close()
        ok = " 204 " in resp.split("\r\n", 1)[0] + " "
        if not ok:
            logger.error("[remediate] docker restart %s -> %s",
                         container, resp[:120])
        return ok
    except Exception as exc:
        logger.error("[remediate] docker socket restart failed: %s", exc)
        return False


def _maybe_clear_kill_switch(healthy_streak: int) -> None:
    """Un-trip a WATCHDOG-TRIPPED balance_staleness halt after sustained
    recovery. Scoped strictly: only clears when the active halt's reason is
    balance_staleness (the self-healing component) — manual halts and every
    other reason stay until an operator clears them.

    Design gap this closes (2026-07-07): a ~1h DNS outage tripped the halt
    at 08:35 UTC; snapshots recovered by 17:18 but the file has no
    auto-clear, so the one profitable signal sat blocked for ~18h with
    thousands of attempts eaten silently.
    """
    if healthy_streak < AUTO_CLEAR_CONSECUTIVE:
        return
    try:
        from trading_platform.polymarket.kill_switch import KillSwitch
        ks = KillSwitch(str(DB_PATH))
        active, reason = ks.is_emergency_stopped()
        if not active or "balance_staleness" not in (reason or ""):
            return
        ks.clear_emergency_stop()
        logger.warning("[deadman] AUTO-CLEAR: balance_staleness halt cleared "
                       "after %d healthy cycles (was: %s)",
                       healthy_streak, (reason or "")[:120])
        _send_alert(
            f"\U0001f7e2 <b>AUTO-CLEAR</b>\nThe balance_staleness kill switch "
            f"un-tripped after {AUTO_CLEAR_CONSECUTIVE} consecutive healthy "
            f"balance checks (~{AUTO_CLEAR_CONSECUTIVE * POLL_INTERVAL_SECONDS // 60} min).\n"
            f"Was: {(reason or '')[:160]}\nLive entries resume.", loud=True)
    except Exception as exc:
        logger.error("[deadman] auto-clear failed: %s", exc)


def _trip_kill_switch(reason: str) -> None:
    """Write the KILL_SWITCH_ACTIVE flag via the canonical kill_switch API so
    the live executor halts new entries. Idempotent-ish (emergency_stop just
    rewrites the flag)."""
    try:
        from trading_platform.polymarket.kill_switch import KillSwitch
        ks = KillSwitch(str(DB_PATH))
        already, _ = ks.is_emergency_stopped()
        if not already:
            ks.emergency_stop(reason=reason)
            logger.warning("[deadman] AUTO-HALT engaged: %s", reason)
            _send_alert(
                f"\U0001f6d1 <b>AUTO-HALT</b>\nLive trading halted by watchdog\n"
                f"Reason: {reason}\nClear via /api or remove data/KILL_SWITCH_ACTIVE "
                f"once resolved.", loud=True)
    except Exception as exc:
        logger.error("[deadman] failed to trip kill switch: %s", exc)


def main() -> None:
    startup_ts = time.time()
    # N1 (d): warn loudly at startup if the off-host dead-man's switch is unarmed.
    if not HEARTBEAT_PING_URL:
        logger.warning(
            "[deadman] HEARTBEAT_PING_URL UNSET — a host-down event (power "
            "off / crash / network loss) is NOT detectable off-host. Set it to "
            "a healthchecks.io ping URL in .env.")
        _send_alert(
            "⚠️ <b>Dead-man's switch UNARMED</b>\nHEARTBEAT_PING_URL is "
            "not set — if the host dies, nothing off-host will notice. Set it to "
            "a healthchecks.io URL in .env.", loud=True)
    _last_alive_ping = time.time()
    logger.info(
        "health watchdog starting (poll %ds, %d consecutive failures to alert, %ds grace)",
        POLL_INTERVAL_SECONDS, CONSECUTIVE_FAILURES_TO_ALERT, STARTUP_GRACE_SECONDS,
    )
    failure_state: dict[str, float] = {}  # component_name → first_failure_ts
    fail_count: dict[str, int] = {}       # consecutive-failure counter
    last_alert_ts: dict[str, float] = {}  # per-component re-alert throttle
    last_remediation: dict[str, float] = {}  # auto-remediation cooldown
    balance_healthy_streak = 0            # for the scoped halt auto-clear
    while True:
        now = time.time()
        states = [check_api(), check_db(), check_scheduler(),
                  check_critical_loops(),
                  check_live_collect(), check_wallet_stream(), check_disk(),
                  check_hypothesis_drift(),
                  check_scheduler_consecutive_failures(),
                  check_balance_staleness(), check_poller_freshness(),
                  check_reconcile_age(), check_trading_liveness(),
                  check_firehose()]
        in_grace = (now - startup_ts) < STARTUP_GRACE_SECONDS
        if in_grace and any(not s.healthy for s in states):
            logger.info("[grace] suppressed alerts during startup window")

        # External dead-man's switch — ping off-host so a host-down event
        # (power off, crash, network loss) surfaces even though the in-stack
        # watchdog dies with the host. No-op unless HEARTBEAT_PING_URL is set.
        _external_heartbeat(states)

        # Compute new failures and recoveries. Require N consecutive cycles
        # to avoid alerting on single-cycle blips (container restart, slow
        # healthcheck on startup, etc.).
        for s in states:
            if not s.healthy:
                fail_count[s.name] = fail_count.get(s.name, 0) + 1
            else:
                fail_count.pop(s.name, None)

            if (
                not s.healthy
                and fail_count.get(s.name, 0) >= CONSECUTIVE_FAILURES_TO_ALERT
                and s.name not in failure_state
                and not in_grace
            ):
                failure_state[s.name] = now
                last_alert_ts[s.name] = now
                msg = (
                    f"\U0001f534 <b>SYSTEM ALERT</b>\n"
                    f"Component <b>{s.name}</b> failing\n"
                    f"Detail: {s.detail[:200]}\n\n"
                    + format_status(states)
                )
                logger.warning("[alert] %s failing: %s", s.name, s.detail)
                _send_alert(msg, loud=True)
            elif (
                not s.healthy and s.name in failure_state and not in_grace
                and now - last_alert_ts.get(s.name, 0) >= REALERT_INTERVAL_S
            ):
                # Re-alert on STILL-failing components. The alert-once design
                # sent a single Telegram about a dead scheduler at 01:21 on
                # 2026-07-21 and then nothing for 23 hours.
                last_alert_ts[s.name] = now
                down_h = (now - failure_state[s.name]) / 3600
                logger.warning("[re-alert] %s STILL failing (%.1fh): %s",
                               s.name, down_h, s.detail)
                _send_alert(
                    f"\U0001f534 <b>STILL FAILING</b> ({down_h:.0f}h)\n"
                    f"Component <b>{s.name}</b>\nDetail: {s.detail[:200]}",
                    loud=True)

            # Scheduler auto-remediation: dispatch-dead scheduler gets a
            # container restart via the docker socket (rate-limited).
            if (
                s.name == "scheduler" and not s.healthy and not in_grace
                and fail_count.get(s.name, 0) >= SCHED_REMEDIATE_FAILURES
                and now - last_remediation.get("scheduler", 0) > SCHED_REMEDIATE_COOLDOWN_S
            ):
                last_remediation["scheduler"] = now
                logger.warning("[remediate] scheduler failing %d cycles — "
                               "restarting container", fail_count[s.name])
                ok = _docker_restart("polymarket-scheduler")
                _send_alert(
                    f"\U0001f501 <b>AUTO-REMEDIATION</b>\nScheduler dispatch-dead "
                    f"{fail_count[s.name]} cycles — container restart "
                    f"{'succeeded' if ok else 'FAILED (check docker socket mount)'}.\n"
                    f"Detail: {s.detail[:150]}", loud=True)
            elif s.healthy and s.name in failure_state:
                duration_min = (now - failure_state[s.name]) / 60
                msg = (
                    f"\U0001f7e2 <b>RECOVERED</b>\n"
                    f"<b>{s.name}</b> back online (was down {duration_min:.0f} min)\n\n"
                    + format_status(states)
                )
                logger.info("[recover] %s back online", s.name)
                _send_alert(msg, loud=False)
                failure_state.pop(s.name, None)

            # Critical-loops auto-remediation: same docker-socket restart as the
            # scheduler, as a BACKSTOP. The container has its own in-process stall
            # self-heal (os._exit → restart: unless-stopped); this catches the
            # residual case where its heartbeat dies in a way the internal
            # self-heal can't (stall thread itself dead, process fully hung, or
            # container stopped). Rate-limited identically to the scheduler.
            if (
                s.name == "critical_loops" and not s.healthy and not in_grace
                and fail_count.get(s.name, 0) >= SCHED_REMEDIATE_FAILURES
                and now - last_remediation.get("critical_loops", 0) > SCHED_REMEDIATE_COOLDOWN_S
            ):
                last_remediation["critical_loops"] = now
                logger.warning("[remediate] critical-loops failing %d cycles — "
                               "restarting container", fail_count[s.name])
                ok = _docker_restart("polymarket-critical-loops")
                _send_alert(
                    f"\U0001f501 <b>AUTO-REMEDIATION</b>\nCritical-loops runner "
                    f"unhealthy {fail_count[s.name]} cycles — container restart "
                    f"{'succeeded' if ok else 'FAILED (check docker socket mount)'}.\n"
                    f"Detail: {s.detail[:150]}", loud=True)

            # N1 (b): auto-HALT on SUSTAINED failure of a SEVERE component
            # (untrustworthy balance/reconcile state). Higher threshold than a
            # plain alert so a single blip never trips the kill switch.
            if (not s.healthy and s.name in _SEVERE_HALT_COMPONENTS
                    and fail_count.get(s.name, 0) >= HALT_CONSECUTIVE_FAILURES
                    and not in_grace):
                _trip_kill_switch(f"{s.name}: {s.detail[:120]}")

            # Scoped auto-clear: sustained balance recovery un-trips a
            # watchdog-tripped balance_staleness halt (2026-07-08 gap:
            # an already-recovered halt blocked entries for ~18h).
            if s.name == "balance_staleness":
                balance_healthy_streak = (balance_healthy_streak + 1
                                          if s.healthy else 0)
                if not in_grace:
                    _maybe_clear_kill_switch(balance_healthy_streak)

        # N1 (c): daily "still alive" ping — silence itself becomes the alarm.
        if now - _last_alive_ping >= ALIVE_PING_INTERVAL_S:
            _last_alive_ping = now
            healthy_n = sum(1 for s in states if s.healthy)
            # Recurring nag: an UNARMED external switch is the single reason a
            # full-stack-down (wedged Docker engine, host off) goes silent —
            # the in-stack watchdog dies with the stack, so only an off-host
            # ping catches it. On 2026-07-13 that scenario cost ~6.5h dark with
            # zero alert. Warn on every alive-ping, not just startup, until set.
            armed_line = (
                "" if HEARTBEAT_PING_URL else
                "\n\n⚠️ <b>External dead-man's switch UNARMED</b> — "
                "set HEARTBEAT_PING_URL (healthchecks.io) or a full-stack/host "
                "outage will NOT be detectable off-host.")
            _send_alert(
                f"\U0001f7e2 <b>Watchdog alive</b> — {healthy_n}/{len(states)} "
                f"components healthy\n\n" + format_status(states) + armed_line,
                loud=False)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("watchdog stopped")
