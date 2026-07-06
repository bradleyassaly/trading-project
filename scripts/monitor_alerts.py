"""Telegram alert dispatcher for the observability layer.

Polls the four monitors built this week and fires telegram_alerts when
any of them surface anomalies:

  1. api_health — any API silent past per-API threshold
  2. reconciler — DRIFT in latest log
  3. ws_poll_reconcile — VOLUME DROP or WS DEGRADED
  4. wallet_attribution — new auto-demote candidates

Anti-spam: telegram_alerts.send_pipeline_alert has a 30-min cooldown
per component, so we can poll every 5-15 min without spamming.

Runs as a scheduled task. Each fire updates a small monitor_alert_state
table so we don't fire the same alert twice within the cooldown.
"""
from __future__ import annotations

import os
import re
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))


# Per-API freshness threshold — must agree with daily_system_review section 11
API_THRESHOLDS = {
    # 2026-07-06: gamma raised 1h → 8h. Its recorders (markets_refresh,
    # universe indexer, resolution fetcher) run on 6-24h cadences, so a
    # 1h threshold read "stale" between perfectly healthy runs and
    # re-alerted every cooldown window all day. 8h still catches a truly
    # dead gamma within one working day.
    "gamma": 8 * 3600,
    "clob": 600,
    "data_api_trades": 1800,
    "polymarket_other": 3600,
}


def check_api_health(alerter) -> int:
    """Return count of alerts fired.

    2026-05-28 fix: previously `if age is not None and age > thr` fell
    through silently when age was None (meaning last_success_ts is
    NULL — recorder bug OR API has never succeeded). That's the WORST
    failure mode — we don't know if the API is dead or just untracked.
    Now: alert on EITHER stale (age > threshold) OR untracked
    (success_count > 0 but age is None — record_success() bug).
    """
    from trading_platform.polymarket.api_health import get_status
    rows = get_status()
    fired = 0
    for r in rows:
        thr = API_THRESHOLDS.get(r["api_name"], 3600)
        age = r["last_success_age_sec"]
        if age is None and (r.get("success_24h", 0) or 0) > 0:
            # Counter says successes occurred but timestamp is NULL —
            # record_success() upsert is broken or schema migration left
            # rows in an inconsistent state. Alert loudly.
            # Count only alerts actually SENT — send_pipeline_alert
            # returns False when the persisted cooldown suppresses it,
            # and counting those made the log read "fired 1 alert" every
            # run while nothing was being sent.
            if alerter.send_pipeline_alert(
                component=f"api:{r['api_name']}",
                message=(f"UNTRACKED — success_count_24h={r['success_24h']} "
                         f"but last_success_ts is NULL. "
                         f"record_success() upsert is broken; api_health "
                         f"data is unreliable until fixed."),
                level="warning",
            ):
                fired += 1
            continue
        if age is not None and age > thr:
            if alerter.send_pipeline_alert(
                component=f"api:{r['api_name']}",
                message=(f"silent {age//60}m (threshold {thr//60}m). "
                         f"24h: {r['success_24h']} ok / {r['error_24h']} err. "
                         f"Last error: {(r.get('last_error_msg') or 'none')[:80]}"),
                level="critical" if age > 2 * thr else "warning",
            ):
                fired += 1
    return fired


def _read_last_block(path: str, marker: str) -> str | None:
    if not os.path.exists(path): return None
    try:
        with open(path) as f: lines = f.readlines()
        last = None
        for i in range(len(lines)-1, -1, -1):
            if marker in lines[i]: last = i; break
        if last is None: return None
        return "".join(lines[last:last+40])
    except Exception:
        return None


def check_reconciler(alerter) -> int:
    block = _read_last_block("logs/scheduler/reconcile_polymarket_truth.log",
                              "Polymarket-truth reconciliation")
    if not block: return 0
    # Drift means lines starting with "DRIFT — DB does not match"
    if "DRIFT" not in block:
        return 0
    # Extract drift lines
    drift_lines = [ln for ln in block.split("\n") if "diff" in ln.lower() or "DRIFT" in ln]
    msg = "DB-vs-on-chain drift:\n" + "\n".join(drift_lines[:5])
    alerter.send_pipeline_alert(
        component="reconciler", message=msg[:500], level="warning",
    )
    return 1


def check_ws_poll(alerter) -> int:
    block = _read_last_block("logs/scheduler/ws_poll_reconcile.log",
                              "WS-vs-poll wallet")
    if not block: return 0
    if "VOLUME DROP" not in block and "WS DEGRADED" not in block \
            and "CRITICAL" not in block:
        return 0
    alert_lines = [ln.strip() for ln in block.split("\n")
                    if ("ALERT" in ln or "CRITICAL" in ln) and ":" in ln]
    msg = "ingestion degraded:\n" + "\n".join(alert_lines[:3])
    alerter.send_pipeline_alert(
        component="ws_poll", message=msg[:500], level="critical",
    )
    return 1


def check_wallet_attribution(alerter) -> int:
    block = _read_last_block("logs/scheduler/wallet_attribution.log",
                              "PER-WALLET ATTRIBUTION")
    if not block: return 0
    if "AUTO-DEMOTE candidates" not in block: return 0
    # Extract count from line like "AUTO-DEMOTE candidates (N): ..."
    m = re.search(r"AUTO-DEMOTE candidates \((\d+)\)", block)
    if not m or int(m.group(1)) == 0: return 0
    n = int(m.group(1))
    alerter.send_pipeline_alert(
        component="wallet_attribution",
        message=f"{n} wallet(s) flagged for auto-demote (30d PnL <= -$10, n>=5). "
                f"Review log + apply wallet_attribution.py --apply if accepted.",
        level="warning",
    )
    return 1


def main() -> int:
    from trading_platform.polymarket.telegram_alerts import get_alerter
    alerter = get_alerter()
    total = 0
    total += check_api_health(alerter)
    total += check_reconciler(alerter)
    total += check_ws_poll(alerter)
    total += check_wallet_attribution(alerter)
    print(f"monitor_alerts fired {total} alert(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
