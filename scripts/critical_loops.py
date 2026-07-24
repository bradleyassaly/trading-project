"""
Critical-loop runner — the money loop, in its OWN container.

Extracted from scripts/task_scheduler.py on 2026-07-23. The single sequential
scheduler runs ~80 tasks (including a ~50-min batch backtest) and went silently
dispatch-dead FOUR times in five days (2026-07-17 → 22). Every incident took
the money-critical loops down with it, because they shared the scheduler's one
serial dispatch loop. This process runs ONLY those three loops so a batch-job
wedge can never starve them again:

  - live_equity_snapshot  (hourly)  heartbeats the balance-staleness kill
                                    switch AND feeds the circuit breaker's
                                    live-equity view. If it stalls, the kill
                                    switch false-trips and blocks entries.
  - live_position_monitor (5 min)   SL/TP/trailing exits on LIVE positions.
  - maker_experiment      (5 min)   manages resting LIVE maker quotes.

Design (mirrors the scheduler's proven safety machinery, minus the shared-fate
serial loop):

  * Each loop runs in its OWN thread and executes its task as a SUBPROCESS with
    a HARD timeout. One loop hanging cannot block the others (independent
    threads) and cannot wedge forever (subprocess.run enforces the timeout and
    SIGKILLs the child) — the exact doom-loop class that killed the scheduler
    (a task whose timeout was effectively infinite) is structurally impossible
    here because the timeouts are small fixed constants.

  * Stall self-heal: every loop stamps a heartbeat continuously (at the top of
    each cycle and every ~30s while sleeping). A watchdog thread hard-exits the
    process (os._exit) if ANY loop goes silent past CRITICAL_LOOPS_STALL_EXIT_S,
    and docker's restart: unless-stopped revives the container. On restart each
    loop runs immediately, so a fresh equity snapshot lands well inside the
    2h balance-staleness halt window.

  * This container now OWNS the resting maker quotes, so it cancels them on
    BOTH exit paths (SIGTERM and the stall self-restart) — otherwise a restart
    orphans live orders to unmanaged pickoff exposure. The scheduler no longer
    cancels maker quotes (its cancel_all is account-global and its frequent
    restarts would yank THIS container's live book).

  * A service_health row ('critical_loops') is written every ~60s so the health
    watchdog can observe liveness; status flips to 'degraded' when a loop stops
    COMPLETING (fresh heartbeat but no recent success — the wallet_stream
    lesson). The watchdog's balance_staleness + trading_liveness checks verify
    the OUTCOMES (fresh snapshot, live fills).

Run: python scripts/critical_loops.py            # the service
     python scripts/critical_loops.py --selftest # exercise the harness, no money
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

LOG_DIR = PROJECT_ROOT / "logs" / "critical_loops"
LOG_DIR.mkdir(parents=True, exist_ok=True)

try:
    from trading_platform.polymarket.logging_config import setup_logging
    setup_logging(service="critical-loops")
except Exception:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("critical_loops")

SERVICE_KEY = "critical_loops"

# Beat this often while a loop is idle-sleeping between cycles, so the stall
# watchdog sees a live thread regardless of the loop's cadence. Keeps the max
# beat gap ≈ a single task's timeout, well under STALL_EXIT_S.
BEAT_INTERVAL_S = 30

# Self-restart if ANY loop's heartbeat is silent this long. The normal max gap
# between a loop's beats is one task-timeout (~4 min), so 20 min means the
# thread is genuinely dead/wedged. 20 min sits below the 2h balance-staleness
# halt threshold, so a wedge self-heals before it can false-trip the kill switch
# (same reasoning as the scheduler's 25-min stall exit).
STALL_EXIT_S = int(os.environ.get("CRITICAL_LOOPS_STALL_EXIT_S", str(20 * 60)))

# How stale a loop's LAST SUCCESSFUL run may be before we report 'degraded' to
# the watchdog. Per loop = interval * 2 + timeout (tolerates one missed cycle
# plus a full-timeout run), so a loop that runs but keeps failing surfaces.
SUCCESS_STALE_MULT = float(os.environ.get("CRITICAL_LOOPS_SUCCESS_STALE_MULT", "2"))

HEALTH_WRITE_INTERVAL_S = 60


@dataclass
class Loop:
    name: str
    cmd: list[str]
    interval_s: int
    timeout_s: int
    description: str
    # Stagger first runs so the three don't cold-start pandas / hit the CLOB
    # all at once (bounds peak RSS under the 512m container cap).
    start_delay_s: int = 0
    # runtime state (updated by the loop thread)
    last_beat: float = field(default_factory=time.time)
    last_success: float | None = None
    last_status: str = "pending"          # 'ok' | 'failed' | 'timeout' | 'pending'
    last_duration_s: float | None = None
    last_error: str | None = None
    runs: int = 0


LOOPS: list[Loop] = [
    Loop(
        name="live_position_monitor",
        # Live SL/TP/trailing exits (mirrors paper exits). Safe-by-default:
        # a sell failure leaves the position open and logs; never crashes.
        # No-op when there are zero live positions. 270s timeout preserved
        # from the scheduler: the pass sleeps ~10s per pending exit + HTTP,
        # and a kill BETWEEN placing a CLOB order and booking its fill loses
        # the booking — so the timeout must clear a full pass, not chop it.
        cmd=[sys.executable, "-m", "trading_platform.polymarket.live_position_monitor"],
        interval_s=5 * 60,
        timeout_s=270,
        start_delay_s=0,
        description="Live position auto-exit monitor (every 5min)",
    ),
    Loop(
        name="maker_experiment",
        # Resting complement quotes vs dumb flow. DRY-RUN unless
        # MAKER_EXPERIMENT_LIVE=1 in this container's env (env_file: .env).
        # run_cycle reconciles resting orders each pass, so it self-adopts
        # its own book across restarts; per-condition dedup prevents a double
        # cycle (e.g. brief cutover overlap) from double-quoting.
        cmd=[sys.executable, "-m", "trading_platform.polymarket.maker_experiment"],
        interval_s=5 * 60,
        timeout_s=240,
        start_delay_s=5,
        description="Maker experiment: resting complement quotes ($1-scale)",
    ),
    Loop(
        name="live_equity_snapshot",
        # Balance-staleness kill-switch heartbeat + circuit-breaker equity feed.
        # Idempotent: the snapshot row is keyed ON CONFLICT (ts), so a duplicate
        # cycle (cutover overlap) is a harmless no-op.
        cmd=[sys.executable, "scripts/snapshot_live_equity.py"],
        interval_s=60 * 60,
        timeout_s=240,
        start_delay_s=10,
        description="Record live portfolio equity curve (hourly)",
    ),
]


# ── heartbeat / stall self-heal ─────────────────────────────────────────────

def _beat(loop: Loop) -> None:
    loop.last_beat = time.time()


def _run_once(loop: Loop) -> None:
    """Run one cycle of a loop as a subprocess with a hard timeout. Never
    raises — a hang is bounded by the timeout, any error is recorded and the
    loop continues (a transient failure must not kill the thread; genuine
    thread death is what the stall watchdog catches)."""
    started = time.time()
    log_path = LOG_DIR / f"{loop.name}.log"
    logger.info("[run] %s", loop.name)
    try:
        result = subprocess.run(
            loop.cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=loop.timeout_s,
        )
        elapsed = time.time() - started
        ts = datetime.now(tz=timezone.utc).isoformat()
        try:
            with log_path.open("a", encoding="utf-8") as f:
                f.write(f"\n=== {ts} (exit={result.returncode}, {elapsed:.1f}s) ===\n")
                f.write(result.stdout or "")
        except Exception:
            pass
        loop.runs += 1
        loop.last_duration_s = round(elapsed, 1)
        if result.returncode == 0:
            loop.last_status = "ok"
            loop.last_success = started
            loop.last_error = None
            logger.info("[ok ] %s in %.1fs", loop.name, elapsed)
        else:
            loop.last_status = "failed"
            loop.last_error = (result.stdout or "")[-400:]
            logger.warning("[fail] %s exit=%d", loop.name, result.returncode)
    except subprocess.TimeoutExpired:
        loop.last_status = "timeout"
        loop.last_duration_s = time.time() - started
        loop.last_error = f"timeout after {loop.timeout_s}s"
        logger.warning("[timeout] %s (>%ds)", loop.name, loop.timeout_s)
    except Exception as exc:  # noqa: BLE001 — a loop thread must never die on a cycle error
        loop.last_status = "failed"
        loop.last_duration_s = time.time() - started
        loop.last_error = str(exc)[:400]
        logger.exception("[error] %s", loop.name)


def _loop_thread(loop: Loop) -> None:
    if loop.start_delay_s:
        time.sleep(loop.start_delay_s)
    while True:
        cycle_start = time.time()
        _beat(loop)
        _run_once(loop)
        # Fixed-rate cadence: next cycle at cycle_start + interval. Beat every
        # BEAT_INTERVAL_S while sleeping so the thread stays visibly alive.
        next_start = cycle_start + loop.interval_s
        while True:
            now = time.time()
            if now >= next_start:
                break
            _beat(loop)
            time.sleep(min(BEAT_INTERVAL_S, next_start - now))


def _write_health() -> None:
    """Write a service_health row so the watchdog can observe this container.
    status='degraded' when a loop stops COMPLETING (fresh heartbeat but stale
    last-success) — freshness alone is not enough (wallet_stream wrote 'ok' for
    a week while delivering nothing)."""
    now = time.time()
    stale: list[str] = []
    parts: list[str] = []
    for lp in LOOPS:
        base = lp.last_success if lp.last_success is not None else _START_TS
        age = now - base
        threshold = lp.interval_s * SUCCESS_STALE_MULT + lp.timeout_s
        if age > threshold:
            stale.append(lp.name)
        parts.append(
            f"{lp.name}={lp.last_status}"
            + (f"/{lp.last_duration_s:.0f}s" if lp.last_duration_s is not None else "")
            + f"/age{age/60:.0f}m/n{lp.runs}"
        )
    status = "degraded" if stale else "ok"
    detail = ("stale:" + ",".join(stale) + " | " if stale else "") + " ".join(parts)
    try:
        from trading_platform.polymarket.db_connection import get_connection
        conn = get_connection()
        try:
            conn.execute(
                "INSERT INTO service_health (service, status, error_message, checked_at) "
                "VALUES (?, ?, ?, ?)",
                (SERVICE_KEY, status, detail[:500], int(now)),
            )
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as exc:
        logger.debug("[health] write failed: %s", exc)


def _cancel_maker_quotes(reason: str) -> None:
    """Pull resting LIVE maker quotes off the book (best-effort, ~8s). Called on
    BOTH exit paths — SIGTERM and the stall self-restart — so a dying container
    never orphans live orders to unmanaged pickoff exposure. No-op unless the
    maker is armed live."""
    if os.environ.get("MAKER_EXPERIMENT_LIVE", "0").strip() not in ("1", "true", "yes"):
        return
    logger.warning("[shutdown] %s — cancelling resting maker quotes", reason)
    try:
        subprocess.run(
            [sys.executable, "-m",
             "trading_platform.polymarket.maker_experiment", "--cancel-all"],
            timeout=8,
        )
    except Exception as exc:
        logger.error("[shutdown] cancel-all failed: %s", exc)


def _stall_watchdog() -> None:
    """Self-terminate (→ docker restart) if any loop's heartbeat goes silent,
    and write the service_health row each cycle. Mirrors the scheduler's
    stall self-heal, per loop."""
    last_check = time.time()
    while True:
        time.sleep(min(BEAT_INTERVAL_S, HEALTH_WRITE_INTERVAL_S))
        now = time.time()
        wall_jump = now - last_check
        last_check = now
        # Our own sleep overran by >5min → the HOST slept/suspended (this runs
        # on a desktop). Re-arm every loop instead of firing a misleading
        # self-restart; the machine was gone, the threads weren't wedged.
        if wall_jump > 300:
            for lp in LOOPS:
                _beat(lp)
            continue
        _write_health()
        for lp in LOOPS:
            silent = now - lp.last_beat
            if silent > STALL_EXIT_S:
                msg = (f"[stall-watchdog] loop '{lp.name}' silent "
                       f"{silent/60:.0f}m (> {STALL_EXIT_S//60}m) — self-terminating "
                       f"so docker restarts critical-loops")
                logger.critical(msg)
                _dump_traceback(lp.name, silent)
                _alert(f"🔁 CRITICAL-LOOPS SELF-RESTART\n{msg}")
                # os._exit bypasses SIGTERM, so cancel resting maker quotes here
                # too — else every self-restart orphans live orders.
                _cancel_maker_quotes("stall-self-restart")
                os._exit(42)


def _dump_traceback(loop_name: str, silent_s: float) -> None:
    try:
        import faulthandler
        dump = PROJECT_ROOT / "logs" / "critical_loops_stall_traceback.txt"
        dump.parent.mkdir(parents=True, exist_ok=True)
        with open(dump, "a") as fh:
            fh.write(f"\n===== stall dump {datetime.now(timezone.utc).isoformat()} "
                     f"loop={loop_name} silent {silent_s/60:.0f}m =====\n")
            faulthandler.dump_traceback(file=fh)
    except Exception:
        pass


def _alert(message: str) -> None:
    try:
        from trading_platform.polymarket.telegram_alerts import get_alerter
        a = get_alerter()
        if a.enabled:
            a._send(message, disable_notification=False)
    except Exception:
        pass


def _graceful_shutdown(signum, frame) -> None:
    """SIGTERM (docker stop / host shutdown, ~10s grace): pull resting maker
    quotes off the book before dying. This host is a DESKTOP that sleeps and
    restarts — while it's down nobody manages the quotes."""
    _cancel_maker_quotes(f"signal {signum}")
    os._exit(0)


_START_TS = time.time()


def main() -> None:
    logger.info("critical-loops starting: %d loops, stall self-heal %dm",
                len(LOOPS), STALL_EXIT_S // 60)
    import signal
    signal.signal(signal.SIGTERM, _graceful_shutdown)
    threading.Thread(target=_stall_watchdog, daemon=True, name="stall-watchdog").start()
    threads = []
    for lp in LOOPS:
        t = threading.Thread(target=_loop_thread, args=(lp,), daemon=True,
                             name=f"loop-{lp.name}")
        t.start()
        threads.append(t)
    # Keep the main thread alive; the daemon threads do the work and the stall
    # watchdog handles restart. Join with a timeout loop so KeyboardInterrupt
    # is responsive.
    while True:
        time.sleep(3600)


def _selftest() -> int:
    """Exercise the loop/subprocess/timeout/heartbeat harness with trivial
    commands — NO money operations. Verifies: a fast command records 'ok', a
    hanging command is killed at the timeout, and heartbeats advance."""
    print("[selftest] running harness checks (no live tasks)...")
    ok = Loop(name="probe_ok", cmd=[sys.executable, "-c", "print('probe ok')"],
              interval_s=1, timeout_s=10, description="probe")
    _run_once(ok)
    assert ok.last_status == "ok", f"expected ok, got {ok.last_status}"
    assert ok.last_success is not None
    print(f"[selftest] fast probe -> {ok.last_status} in {ok.last_duration_s}s ✓")

    hang = Loop(name="probe_hang",
                cmd=[sys.executable, "-c", "import time; time.sleep(999)"],
                interval_s=1, timeout_s=2, description="probe")
    t0 = time.time()
    _run_once(hang)
    took = time.time() - t0
    assert hang.last_status == "timeout", f"expected timeout, got {hang.last_status}"
    assert took < 10, f"timeout not enforced (took {took:.1f}s)"
    print(f"[selftest] hang probe killed at timeout in {took:.1f}s -> "
          f"{hang.last_status} ✓")

    b0 = ok.last_beat
    _beat(ok)
    assert ok.last_beat >= b0
    print("[selftest] heartbeat advances ✓")
    print("[selftest] PASS")
    return 0


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        raise SystemExit(_selftest())
    try:
        main()
    except KeyboardInterrupt:
        logger.info("critical-loops stopped")
