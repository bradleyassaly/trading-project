"""
Slow-lane runner — heavy analytics jobs in their OWN container.

Created 2026-07-27. Three forces demanded it:
  * wallet_strategy_observer / wallet_behavior_metrics / wallet_profiles_rebuild
    were dying at the scheduler's 15-min hard task cap (_MAX_TASK_TIMEOUT_S,
    added 2026-07-23 to kill the backtest doom-loop) — the observer had 9
    consecutive cap-kills when this file was written.
  * two of those jobs had ALSO been OOM-killed under the scheduler's 2 GiB
    container limit.
  * the same day, an ad-hoc heavy job exec'd INTO the scheduler container
    starved the dispatch loop badly enough that the watchdog auto-restarted
    the container mid-job (the firehose retention catch-up incident).

The fix is structural, mirroring critical_loops.py from the other side of
the spectrum: money loops got their own container so batch work can't
starve them; heavy batch work now gets ITS own container so it can run for
an hour with real memory without threatening either the dispatch loop or
the money loops. This runner is intentionally SEQUENTIAL — one job at a
time — because concurrent heavy jobs stacking RAM is precisely the OOM
mode that killed them in the scheduler.

Design (idioms shared with critical_loops.py):
  * each job runs as a SUBPROCESS with a hard (but generous, 60-min)
    timeout; output appends to logs/slow_lane/<name>.log
  * a heartbeat thread stamps liveness; the process hard-exits (os._exit)
    if the runner thread wedges past STALL_EXIT_S, and docker
    restart: unless-stopped revives it
  * a service_health row ('slow_lane') is written every ~60s: 'degraded'
    when any job stops COMPLETING (fresh heartbeat but stale last-success —
    the wallet_stream lesson), so the watchdog observes outcomes, not
    just process liveness
  * fixed-rate cadence per job with catch-up-on-restart (a job overdue at
    boot runs immediately; start_delay staggers the cold start)

Run: python scripts/slow_lane.py             # the service
     python scripts/slow_lane.py --selftest  # harness smoke, no real jobs
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

LOG_DIR = PROJECT_ROOT / "logs" / "slow_lane"
LOG_DIR.mkdir(parents=True, exist_ok=True)

try:
    from trading_platform.polymarket.logging_config import setup_logging
    setup_logging(service="slow-lane")
except Exception:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("slow_lane")

SERVICE_KEY = "slow_lane"
BEAT_INTERVAL_S = 30
# The runner is sequential, so the max legitimate beat gap is one job's
# timeout. Stall exit = timeout + slack.
JOB_TIMEOUT_S = int(os.environ.get("SLOW_LANE_JOB_TIMEOUT_S", str(60 * 60)))
STALL_EXIT_S = int(os.environ.get("SLOW_LANE_STALL_EXIT_S",
                                  str(JOB_TIMEOUT_S + 10 * 60)))
SUCCESS_STALE_MULT = float(os.environ.get("SLOW_LANE_SUCCESS_STALE_MULT", "2"))
HEALTH_WRITE_INTERVAL_S = 60


@dataclass
class Job:
    name: str
    cmd: list[str]
    interval_s: int
    description: str
    start_delay_s: int = 0
    # runtime state
    last_attempt: float | None = None
    last_success: float | None = None
    last_status: str = "pending"
    last_duration_s: float | None = None
    last_error: str | None = None
    runs: int = 0


JOBS: list[Job] = [
    Job(
        name="update_exit_policy",
        # A3 exit counterfactual + per-trade persistence (2026-07-27) —
        # moved from the scheduler: its per-trade backfill pass and the
        # 4-source payout waterfall grow with history.
        cmd=[sys.executable, "-m",
             "trading_platform.polymarket.exit_counterfactual", "--apply"],
        interval_s=24 * 3600,
        start_delay_s=60,
        description="Exit-policy counterfactual + per-trade exit_alpha table",
    ),
    Job(
        name="wallet_strategy_observer",
        # 9 consecutive 15-min cap-kills in the scheduler (2026-07-27).
        cmd=[sys.executable, "-m",
             "trading_platform.polymarket.wallet_strategy_observer"],
        interval_s=12 * 3600,
        start_delay_s=5 * 60,
        description="Per-wallet × per-strategy alpha attribution",
    ),
    Job(
        name="wallet_behavior_metrics",
        # Cap-killed 4x in the scheduler; also previously OOM'd at 2 GiB.
        cmd=[sys.executable, "-m",
             "trading_platform.polymarket.wallet_behavior_metrics"],
        interval_s=24 * 3600,
        start_delay_s=10 * 60,
        description="Behavioral metrics: bootstrap-CI, z-score, sizing, sybil",
    ),
    Job(
        name="wallet_profiles_rebuild",
        # Cap-killed in the scheduler (900s at 15-min cap).
        cmd=["trading-cli", "data", "polymarket", "wallet-profiles",
             "--from-db"],
        interval_s=24 * 3600,
        start_delay_s=15 * 60,
        description="Recompute wallet metrics from the trade history",
    ),
]

_START_TS = time.time()
_LAST_BEAT = time.time()


def _beat() -> None:
    global _LAST_BEAT
    _LAST_BEAT = time.time()


def _run_once(job: Job) -> None:
    """One cycle as a subprocess with a hard timeout. Never raises."""
    started = time.time()
    job.last_attempt = started
    log_path = LOG_DIR / f"{job.name}.log"
    logger.info("[run] %s", job.name)
    try:
        result = subprocess.run(
            job.cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=JOB_TIMEOUT_S,
        )
        elapsed = time.time() - started
        ts = datetime.now(tz=timezone.utc).isoformat()
        try:
            with log_path.open("a", encoding="utf-8") as f:
                f.write(f"\n=== {ts} (exit={result.returncode}, {elapsed:.1f}s) ===\n")
                f.write((result.stdout or "")[-20000:])
        except Exception:
            pass
        job.runs += 1
        job.last_duration_s = round(elapsed, 1)
        if result.returncode == 0:
            job.last_status = "ok"
            job.last_success = started
            job.last_error = None
            logger.info("[ok ] %s in %.1fs", job.name, elapsed)
        else:
            job.last_status = "failed"
            job.last_error = (result.stdout or "")[-400:]
            logger.warning("[fail] %s exit=%d", job.name, result.returncode)
    except subprocess.TimeoutExpired as texc:
        job.last_status = "timeout"
        job.last_duration_s = time.time() - started
        job.last_error = f"timeout after {JOB_TIMEOUT_S}s"
        logger.warning("[timeout] %s (>%ds)", job.name, JOB_TIMEOUT_S)
        # Write whatever the child produced — a timeout with an empty log
        # is undiagnosable (wallet_strategy_observer's first 3600s timeout
        # left zero output to reason from).
        try:
            partial = texc.output or b""
            if isinstance(partial, bytes):
                partial = partial.decode("utf-8", "replace")
            with log_path.open("a", encoding="utf-8") as f:
                f.write(f"\n=== {datetime.now(tz=timezone.utc).isoformat()} "
                        f"(TIMEOUT after {JOB_TIMEOUT_S}s) ===\n")
                f.write(partial[-20000:])
        except Exception:
            pass
    except Exception as exc:  # noqa: BLE001 — the runner must never die on a job error
        job.last_status = "failed"
        job.last_duration_s = time.time() - started
        job.last_error = str(exc)[:400]
        logger.exception("[error] %s", job.name)


def _due(job: Job, now: float) -> bool:
    if now - _START_TS < job.start_delay_s:
        return False
    base = job.last_attempt
    if base is None:
        return True  # overdue-at-boot runs immediately (after its stagger)
    return now - base >= job.interval_s


def _runner() -> None:
    """Sequential dispatch: one heavy job at a time, ever."""
    while True:
        _beat()
        now = time.time()
        ran = False
        for job in JOBS:
            if _due(job, now):
                _run_once(job)
                _beat()
                ran = True
                break  # re-evaluate due-ness after each job
        if not ran:
            time.sleep(BEAT_INTERVAL_S)


def _write_health() -> None:
    now = time.time()
    stale: list[str] = []
    parts: list[str] = []
    for j in JOBS:
        base = j.last_success if j.last_success is not None else _START_TS
        age = now - base
        threshold = j.interval_s * SUCCESS_STALE_MULT + JOB_TIMEOUT_S
        if age > threshold:
            stale.append(j.name)
        parts.append(
            f"{j.name}={j.last_status}"
            + (f"/{j.last_duration_s:.0f}s" if j.last_duration_s is not None else "")
            + f"/age{age / 3600:.1f}h/n{j.runs}"
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


def _watchdog_thread() -> None:
    """Health writes + stall self-heal for the (single) runner thread."""
    last_health = 0.0
    while True:
        time.sleep(5)
        now = time.time()
        if now - _LAST_BEAT > STALL_EXIT_S:
            logger.error("[stall] runner silent %.0fs > %ds — self-exiting "
                         "for docker restart", now - _LAST_BEAT, STALL_EXIT_S)
            os._exit(70)
        if now - last_health >= HEALTH_WRITE_INTERVAL_S:
            _write_health()
            last_health = now


def main() -> int:
    if "--selftest" in sys.argv:
        # Harness smoke: replace jobs with trivial commands, run one pass.
        global JOBS
        JOBS = [
            Job(name="st_ok", cmd=[sys.executable, "-c", "print('ok')"],
                interval_s=3600, description="selftest ok"),
            Job(name="st_fail", cmd=[sys.executable, "-c", "raise SystemExit(3)"],
                interval_s=3600, description="selftest fail"),
        ]
        for j in JOBS:
            _run_once(j)
        assert JOBS[0].last_status == "ok", JOBS[0]
        assert JOBS[1].last_status == "failed", JOBS[1]
        _write_health()
        print("selftest pass: ok-path + fail-path + health write")
        return 0

    logger.info("[slow-lane] starting: %s",
                ", ".join(f"{j.name}({j.interval_s // 3600}h)" for j in JOBS))
    t = threading.Thread(target=_watchdog_thread, daemon=True,
                         name="slowlane-watchdog")
    t.start()
    _runner()  # blocks forever
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
