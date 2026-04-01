from __future__ import annotations

import dataclasses
import json
import logging
import time
import urllib.request
import urllib.error
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from trading_platform.config.loader import load_automated_orchestration_config
from trading_platform.orchestration.pipeline_runner import (
    AutomatedOrchestrationConfig,
    run_automated_orchestration,
)

_log = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AutonomousLoopConfig:
    """Configuration for the autonomous orchestration loop."""

    # Required
    orchestration_config_path: str
    cron_schedule: str  # 5-field cron expression e.g. "0 8 * * 1-5"

    # Decision log
    decision_log_path: str = "artifacts/autonomous_loop/decision_log.jsonl"

    # Kill switch
    kill_switch_path: str = "KILL_SWITCH"

    # Staleness thresholds (hours)
    data_staleness_threshold_hours: float = 26.0
    features_staleness_threshold_hours: float = 26.0
    research_staleness_threshold_hours: float = 168.0  # 1 week

    # Roots to scan for artifact staleness
    data_root: str = "data/market_data"
    features_root: str = "data/feature_store"
    research_root: str = "artifacts"

    # Circuit breaker — scans for paper_equity_curve.csv under this root
    circuit_breaker_enabled: bool = True
    circuit_breaker_max_drawdown: float = 0.20  # 20% peak-to-trough
    paper_output_root: str = "artifacts"

    # Performance degradation detection
    degradation_detection_enabled: bool = True
    degradation_monitoring_root: str = "artifacts"
    # fraction of strategies with "deactivate" recommendation to trigger full re-research
    degradation_deactivate_fraction: float = 0.25

    # Alerts
    alert_log_path: str = "artifacts/autonomous_loop/alerts.log"
    alert_webhook_url: str | None = None

    # Loop control
    dry_run: bool = False
    max_iterations: int | None = None

    def __post_init__(self) -> None:
        try:
            _validate_cron_expr(self.cron_schedule)
        except ValueError as exc:
            raise ValueError(f"Invalid cron_schedule: {exc}") from exc
        if not (0 < self.circuit_breaker_max_drawdown < 1):
            raise ValueError("circuit_breaker_max_drawdown must be between 0 and 1 exclusive")
        if not (0 < self.degradation_deactivate_fraction <= 1):
            raise ValueError("degradation_deactivate_fraction must be in (0, 1]")


def load_autonomous_loop_config(path: str | Path) -> AutonomousLoopConfig:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Autonomous loop config not found: {path}")
    with open(p, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    valid = {f.name for f in dataclasses.fields(AutonomousLoopConfig)}
    filtered = {k: v for k, v in data.items() if k in valid}
    return AutonomousLoopConfig(**filtered)


# ─── Decision log ──────────────────────────────────────────────────────────────


@dataclass
class DecisionLogEntry:
    """One record in decision_log.jsonl. Every action the loop takes must
    produce at least one entry so the log is self-contained for audit."""

    timestamp: str
    iteration: int
    loop_run_id: str
    trigger: str        # "scheduled" | "degradation_detected" | "startup" | "shutdown"
    reasoning: str
    action_taken: str   # "full_research" | "fast_refresh" | "skip" | "circuit_breaker_halt" | "kill_switch_exit"
    outcome: str        # "pending" | "succeeded" | "failed" | "skipped" | "dry_run"
    details: dict[str, Any] = field(default_factory=dict)
    orch_run_id: str | None = None
    error: str | None = None


def _write_decision(entry: DecisionLogEntry, log_path: str) -> None:
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(asdict(entry), default=str) + "\n")


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


# ─── Cron scheduling ───────────────────────────────────────────────────────────


def _parse_cron_field(token: str, lo: int, hi: int) -> set[int]:
    """Expand one cron field token into a set of matching integers.
    Supports: * */N A-B A-B/N A,B,C and combinations."""
    result: set[int] = set()
    for part in token.split(","):
        if part == "*":
            result.update(range(lo, hi + 1))
        elif part.startswith("*/"):
            step = int(part[2:])
            result.update(range(lo, hi + 1, step))
        elif "-" in part:
            dash_part, _, step_part = part.partition("/")
            a, _, b = dash_part.partition("-")
            step = int(step_part) if step_part else 1
            result.update(range(int(a), int(b) + 1, step))
        else:
            result.add(int(part))
    return result


def _validate_cron_expr(expr: str) -> None:
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError(f"Expected 5 fields, got {len(parts)}: {expr!r}")
    ranges = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)]
    for part, (lo, hi) in zip(parts, ranges):
        vals = _parse_cron_field(part, lo, hi)
        if not vals:
            raise ValueError(f"Field {part!r} produced no valid values in [{lo},{hi}]")


def _field_is_wildcard(token: str) -> bool:
    return token in {"*", "*/1"}


def _next_cron_time(cron_expr: str, after: datetime) -> datetime:
    """Return the next UTC datetime (after `after`) that matches the cron expression.

    Cron DOW uses 0=Sunday through 6=Saturday.
    Python weekday() uses 0=Monday; conversion: cron_dow -> python: (cron_dow - 1) % 7.

    POSIX day-matching semantics:
      - dom=* and dow=specific  → match dow only
      - dom=specific and dow=*  → match dom only
      - both specific           → match if either matches (OR)
      - both wildcard           → any day matches
    """
    parts = cron_expr.split()
    minutes = sorted(_parse_cron_field(parts[0], 0, 59))
    hours = sorted(_parse_cron_field(parts[1], 0, 23))
    doms = _parse_cron_field(parts[2], 1, 31)
    months = _parse_cron_field(parts[3], 1, 12)
    # Convert cron DOW (0=Sun) to Python weekday (0=Mon)
    dows_py = {(d - 1) % 7 for d in _parse_cron_field(parts[4], 0, 6)}

    dom_wildcard = _field_is_wildcard(parts[2])
    dow_wildcard = _field_is_wildcard(parts[4])

    def _day_matches(dt: datetime) -> bool:
        dom_ok = dt.day in doms
        dow_ok = dt.weekday() in dows_py
        if dom_wildcard and not dow_wildcard:
            return dow_ok
        if not dom_wildcard and dow_wildcard:
            return dom_ok
        if not dom_wildcard and not dow_wildcard:
            return dom_ok or dow_ok
        return True  # both wildcard

    # Start searching from the next whole minute
    candidate = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
    limit = after + timedelta(days=366)

    while candidate < limit:
        # Advance month
        if candidate.month not in months:
            valid_months = sorted(m for m in months if m > candidate.month)
            if valid_months:
                candidate = candidate.replace(month=valid_months[0], day=1, hour=0, minute=0)
            else:
                candidate = candidate.replace(year=candidate.year + 1, month=min(months), day=1, hour=0, minute=0)
            continue

        # Advance day
        if not _day_matches(candidate):
            candidate = candidate.replace(hour=0, minute=0) + timedelta(days=1)
            continue

        # Advance hour
        if candidate.hour not in set(hours):
            valid_hours = [h for h in hours if h > candidate.hour]
            if valid_hours:
                candidate = candidate.replace(hour=valid_hours[0], minute=0)
            else:
                candidate = candidate.replace(hour=0, minute=0) + timedelta(days=1)
            continue

        # Advance minute
        if candidate.minute not in set(minutes):
            valid_mins = [m for m in minutes if m > candidate.minute]
            if valid_mins:
                candidate = candidate.replace(minute=valid_mins[0])
            else:
                hi_idx = hours.index(candidate.hour) + 1 if candidate.hour in hours else None
                if hi_idx is not None and hi_idx < len(hours):
                    candidate = candidate.replace(hour=hours[hi_idx], minute=minutes[0])
                else:
                    candidate = candidate.replace(hour=0, minute=0) + timedelta(days=1)
            continue

        return candidate

    raise RuntimeError(f"No cron match found within one year for expression: {cron_expr!r}")


# ─── Staleness checking ─────────────────────────────────────────────────────────


def _newest_mtime_hours_ago(root: str) -> float | None:
    """Return hours since the newest file under root was modified, or None if no files."""
    p = Path(root)
    if not p.exists():
        return None
    files = [f for f in p.rglob("*") if f.is_file()]
    if not files:
        return None
    newest_ts = max(f.stat().st_mtime for f in files)
    age_seconds = datetime.now(UTC).timestamp() - newest_ts
    return age_seconds / 3600.0


def _check_staleness(config: AutonomousLoopConfig) -> tuple[str, dict[str, Any]]:
    """Decide whether we need full_research, fast_refresh, or skip based on artifact ages.

    Returns (action, details_dict).
    """
    data_age = _newest_mtime_hours_ago(config.data_root)
    features_age = _newest_mtime_hours_ago(config.features_root)
    research_age = _newest_mtime_hours_ago(config.research_root)

    details: dict[str, Any] = {
        "data_age_hours": data_age,
        "features_age_hours": features_age,
        "research_age_hours": research_age,
        "data_threshold_hours": config.data_staleness_threshold_hours,
        "features_threshold_hours": config.features_staleness_threshold_hours,
        "research_threshold_hours": config.research_staleness_threshold_hours,
    }

    # Missing roots are infinitely stale → full research
    if data_age is None:
        details["reason"] = f"data root missing: {config.data_root}"
        return "full_research", details
    if features_age is None:
        details["reason"] = f"features root missing: {config.features_root}"
        return "full_research", details

    data_stale = data_age > config.data_staleness_threshold_hours
    features_stale = features_age > config.features_staleness_threshold_hours
    research_stale = research_age is None or research_age > config.research_staleness_threshold_hours

    if data_stale or features_stale:
        details["reason"] = (
            f"data stale ({data_age:.1f}h)" if data_stale else f"features stale ({features_age:.1f}h)"
        )
        return "full_research", details

    if research_stale:
        details["reason"] = f"research stale ({research_age:.1f}h)" if research_age is not None else "research root empty"
        return "fast_refresh", details

    details["reason"] = "all artifacts fresh"
    return "fast_refresh", details  # default to fast refresh even when fresh


# ─── Performance degradation detection ─────────────────────────────────────────


def _check_degradation(config: AutonomousLoopConfig) -> tuple[bool, list[str], str]:
    """Scan strategy_monitoring.json artifacts for deactivate recommendations.

    Returns (is_degraded, deactivated_strategy_names, reason_string).
    """
    if not config.degradation_detection_enabled:
        return False, [], "degradation detection disabled"

    root = Path(config.degradation_monitoring_root)
    if not root.exists():
        return False, [], f"monitoring root not found: {config.degradation_monitoring_root}"

    monitoring_files = sorted(root.rglob("strategy_monitoring.json"), key=lambda p: p.stat().st_mtime)
    if not monitoring_files:
        return False, [], "no strategy_monitoring.json found"

    # Use only the most recent file
    latest = monitoring_files[-1]
    try:
        data = json.loads(latest.read_text(encoding="utf-8"))
    except Exception as exc:
        return False, [], f"failed to read {latest}: {exc}"

    recommendations = data.get("kill_switch_recommendations", [])
    total = len(recommendations)
    deactivate_names = [
        r.get("preset_name", r.get("strategy", "unknown"))
        for r in recommendations
        if r.get("recommendation") == "deactivate"
    ]

    if not deactivate_names:
        return False, [], f"no deactivate recommendations in {latest}"

    fraction = len(deactivate_names) / max(total, 1)
    if fraction >= config.degradation_deactivate_fraction:
        reason = (
            f"{len(deactivate_names)}/{total} strategies recommend deactivation "
            f"({fraction:.0%} >= threshold {config.degradation_deactivate_fraction:.0%})"
        )
        return True, deactivate_names, reason

    return False, [], (
        f"{len(deactivate_names)}/{total} deactivate recommendations "
        f"({fraction:.0%} < threshold {config.degradation_deactivate_fraction:.0%})"
    )


# ─── Circuit breaker ───────────────────────────────────────────────────────────


def _check_circuit_breaker(config: AutonomousLoopConfig) -> tuple[bool, float, str]:
    """Compute peak-to-trough drawdown across all paper equity curves under paper_output_root.

    Returns (is_tripped, worst_drawdown_fraction, reason).
    Drawdown is expressed as a negative fraction, e.g. -0.22 means 22% drawdown.
    """
    if not config.circuit_breaker_enabled:
        return False, 0.0, "circuit breaker disabled"

    root = Path(config.paper_output_root)
    if not root.exists():
        return False, 0.0, f"paper output root not found: {config.paper_output_root}"

    curve_files = list(root.rglob("paper_equity_curve.csv"))
    if not curve_files:
        return False, 0.0, "no paper_equity_curve.csv files found"

    import pandas as pd

    frames = []
    for cf in curve_files:
        try:
            df = pd.read_csv(cf)
            if "equity" in df.columns and not df.empty:
                frames.append(df[["equity"]].dropna())
        except Exception:
            pass

    if not frames:
        return False, 0.0, "equity curve files found but could not be read"

    all_equity = pd.concat(frames, ignore_index=True)["equity"]
    peak = all_equity.expanding().max()
    drawdown_series = (all_equity - peak) / peak.where(peak > 0, other=1.0)
    worst_drawdown = float(drawdown_series.min())

    threshold = -config.circuit_breaker_max_drawdown
    if worst_drawdown <= threshold:
        reason = (
            f"paper drawdown {worst_drawdown:.1%} exceeds circuit breaker limit "
            f"{threshold:.1%} ({config.circuit_breaker_max_drawdown:.0%})"
        )
        return True, worst_drawdown, reason

    return False, worst_drawdown, f"drawdown {worst_drawdown:.1%} within limit {threshold:.1%}"


# ─── Alerts ────────────────────────────────────────────────────────────────────


def _send_alert(message: str, config: AutonomousLoopConfig, level: str = "WARNING") -> None:
    """Write alert to log file and optionally POST to a webhook URL."""
    timestamp = _now_utc()
    line = f"[{timestamp}] [{level}] {message}\n"

    # Always write to alert log
    alert_path = Path(config.alert_log_path)
    try:
        alert_path.parent.mkdir(parents=True, exist_ok=True)
        with open(alert_path, "a", encoding="utf-8") as fh:
            fh.write(line)
    except Exception as exc:
        _log.error("Failed to write alert log: %s", exc)

    _log.warning("ALERT [%s]: %s", level, message)

    if not config.alert_webhook_url:
        return

    payload = json.dumps({"text": f"[{level}] {message}", "timestamp": timestamp}).encode()
    req = urllib.request.Request(
        config.alert_webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
    except Exception as exc:
        _log.error("Webhook alert delivery failed: %s", exc)
        try:
            with open(alert_path, "a", encoding="utf-8") as fh:
                fh.write(f"[{_now_utc()}] [ERROR] Webhook delivery failed: {exc}\n")
        except Exception:
            pass


# ─── Orchestration dispatch ────────────────────────────────────────────────────


def _build_fast_refresh_config(base: AutomatedOrchestrationConfig) -> AutomatedOrchestrationConfig:
    """Return a copy of base with research/registry/validation/promotion disabled."""
    fast_stages = dataclasses.replace(
        base.stages,
        research=False,
        registry=False,
        validation=False,
        promotion=False,
    )
    return dataclasses.replace(base, stages=fast_stages)


def _dispatch(
    action: str,
    base_orch_config: AutomatedOrchestrationConfig,
    dry_run: bool,
) -> tuple[str, str | None, dict[str, Any]]:
    """Execute the orchestration action.

    Returns (outcome, orch_run_id, details).
    outcome: "succeeded" | "failed" | "skipped" | "dry_run"
    """
    if action == "skip":
        return "skipped", None, {}

    if dry_run:
        _log.info("DRY RUN: would execute action=%s", action)
        return "dry_run", None, {"dry_run": True, "action": action}

    orch_config = (
        base_orch_config if action == "full_research" else _build_fast_refresh_config(base_orch_config)
    )

    try:
        result, artifact_paths = run_automated_orchestration(orch_config)
        details: dict[str, Any] = {
            "stages": {r.stage_name: r.status for r in result.stage_records},
            "warnings": result.warnings,
            "artifact_paths": {k: str(v) for k, v in artifact_paths.items()},
        }
        return result.status, result.run_id, details
    except Exception as exc:
        _log.exception("Orchestration dispatch failed")
        return "failed", None, {"exception": str(exc)}


# ─── Main loop ─────────────────────────────────────────────────────────────────


def run_autonomous_loop(config: AutonomousLoopConfig) -> None:
    """Run the autonomous orchestration loop.

    Blocks until:
    - KILL_SWITCH file is detected
    - Circuit breaker trips (halts trading activity)
    - max_iterations is reached
    - Unrecoverable error

    Every decision is written to config.decision_log_path as a JSON line.
    """
    loop_run_id = uuid.uuid4().hex[:12]
    iteration = 0

    _log.info("Autonomous loop starting (loop_run_id=%s, schedule=%s)", loop_run_id, config.cron_schedule)

    # Ensure output dirs exist
    Path(config.decision_log_path).parent.mkdir(parents=True, exist_ok=True)
    Path(config.alert_log_path).parent.mkdir(parents=True, exist_ok=True)

    # Load the inner orchestration config once — fail fast if it's broken
    base_orch_config = load_automated_orchestration_config(config.orchestration_config_path)

    # Startup log entry
    _write_decision(
        DecisionLogEntry(
            timestamp=_now_utc(),
            iteration=iteration,
            loop_run_id=loop_run_id,
            trigger="startup",
            reasoning=f"Autonomous loop started with schedule={config.cron_schedule!r}",
            action_taken="startup",
            outcome="succeeded",
            details={"orchestration_config": config.orchestration_config_path, "dry_run": config.dry_run},
        ),
        config.decision_log_path,
    )

    while True:
        # ── Kill switch check ──────────────────────────────────────────────────
        if Path(config.kill_switch_path).exists():
            entry = DecisionLogEntry(
                timestamp=_now_utc(),
                iteration=iteration,
                loop_run_id=loop_run_id,
                trigger="shutdown",
                reasoning=f"KILL_SWITCH file detected at {config.kill_switch_path}",
                action_taken="kill_switch_exit",
                outcome="succeeded",
            )
            _write_decision(entry, config.decision_log_path)
            _send_alert(f"KILL_SWITCH file detected — autonomous loop exiting cleanly.", config, level="INFO")
            _log.info("KILL_SWITCH detected. Exiting loop.")
            return

        # ── Sleep until next cron tick ─────────────────────────────────────────
        now = datetime.now(UTC)
        try:
            next_fire = _next_cron_time(config.cron_schedule, after=now)
        except RuntimeError as exc:
            _send_alert(f"Cron schedule error: {exc}", config, level="ERROR")
            return

        wait_seconds = (next_fire - now).total_seconds()
        _log.info(
            "Iteration %d: sleeping %.0fs until %s",
            iteration,
            wait_seconds,
            next_fire.isoformat(),
        )
        if wait_seconds > 0 and not config.dry_run:
            # Poll kill switch every 60s during sleep
            slept = 0.0
            while slept < wait_seconds:
                chunk = min(60.0, wait_seconds - slept)
                time.sleep(chunk)
                slept += chunk
                if Path(config.kill_switch_path).exists():
                    break

        # ── Kill switch re-check after sleep ───────────────────────────────────
        if Path(config.kill_switch_path).exists():
            entry = DecisionLogEntry(
                timestamp=_now_utc(),
                iteration=iteration,
                loop_run_id=loop_run_id,
                trigger="shutdown",
                reasoning=f"KILL_SWITCH file detected after sleep at {config.kill_switch_path}",
                action_taken="kill_switch_exit",
                outcome="succeeded",
            )
            _write_decision(entry, config.decision_log_path)
            _send_alert("KILL_SWITCH file detected after sleep — autonomous loop exiting cleanly.", config, level="INFO")
            return

        # ── Circuit breaker ────────────────────────────────────────────────────
        cb_tripped, cb_drawdown, cb_reason = _check_circuit_breaker(config)
        if cb_tripped:
            entry = DecisionLogEntry(
                timestamp=_now_utc(),
                iteration=iteration,
                loop_run_id=loop_run_id,
                trigger="circuit_breaker",
                reasoning=cb_reason,
                action_taken="circuit_breaker_halt",
                outcome="succeeded",
                details={"drawdown": cb_drawdown, "threshold": -config.circuit_breaker_max_drawdown},
            )
            _write_decision(entry, config.decision_log_path)
            _send_alert(
                f"CIRCUIT BREAKER TRIPPED: {cb_reason}. All trading activity halted.",
                config,
                level="CRITICAL",
            )
            _log.critical("Circuit breaker tripped: %s", cb_reason)
            return

        # ── Determine trigger and action ───────────────────────────────────────
        degraded, degraded_names, degradation_reason = _check_degradation(config)
        stale_level, stale_details = _check_staleness(config)

        if degraded:
            trigger = "degradation_detected"
            action = "full_research"
            reasoning = f"Performance degradation: {degradation_reason}. Triggering full re-research."
        elif stale_level == "full_research":
            trigger = "scheduled"
            action = "full_research"
            reasoning = f"Artifact staleness requires full research. {stale_details.get('reason', '')}"
        else:
            trigger = "scheduled"
            action = "fast_refresh"
            reasoning = f"Running fast refresh (skip research stages). {stale_details.get('reason', '')}"

        _log.info("Iteration %d: trigger=%s action=%s reasoning=%s", iteration, trigger, action, reasoning)

        # ── Write pre-dispatch decision (pending) ──────────────────────────────
        pre_entry = DecisionLogEntry(
            timestamp=_now_utc(),
            iteration=iteration,
            loop_run_id=loop_run_id,
            trigger=trigger,
            reasoning=reasoning,
            action_taken=action,
            outcome="pending",
            details={
                "degradation": {"detected": degraded, "strategies": degraded_names, "reason": degradation_reason},
                "staleness": stale_details,
                "circuit_breaker": {"drawdown": cb_drawdown, "reason": cb_reason},
                "dry_run": config.dry_run,
            },
        )
        _write_decision(pre_entry, config.decision_log_path)

        # ── Dispatch ───────────────────────────────────────────────────────────
        outcome, orch_run_id, dispatch_details = _dispatch(action, base_orch_config, config.dry_run)

        # ── Write post-dispatch decision (final outcome) ───────────────────────
        final_entry = dataclasses.replace(
            pre_entry,
            timestamp=_now_utc(),
            outcome=outcome,
            orch_run_id=orch_run_id,
            details={**pre_entry.details, **dispatch_details},
            error=dispatch_details.get("exception"),
        )
        _write_decision(final_entry, config.decision_log_path)

        # ── Alert on failure ───────────────────────────────────────────────────
        if outcome == "failed":
            _send_alert(
                f"Orchestration run FAILED (iter={iteration}, action={action}): "
                f"{dispatch_details.get('exception', 'unknown error')}",
                config,
                level="ERROR",
            )
        elif degraded:
            _send_alert(
                f"Degradation-triggered re-research completed (iter={iteration}, outcome={outcome}): "
                f"{degradation_reason}",
                config,
                level="WARNING",
            )

        # ── Iteration bookkeeping ──────────────────────────────────────────────
        iteration += 1
        if config.max_iterations is not None and iteration >= config.max_iterations:
            _log.info("Reached max_iterations=%d. Exiting loop.", config.max_iterations)
            _write_decision(
                DecisionLogEntry(
                    timestamp=_now_utc(),
                    iteration=iteration,
                    loop_run_id=loop_run_id,
                    trigger="shutdown",
                    reasoning=f"max_iterations={config.max_iterations} reached",
                    action_taken="exit",
                    outcome="succeeded",
                ),
                config.decision_log_path,
            )
            return

        # In dry_run mode break after one iteration so tests can call this
        if config.dry_run:
            _log.info("DRY RUN: exiting after first iteration.")
            return
