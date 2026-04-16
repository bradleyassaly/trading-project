"""Signal flow anomaly detector.

Catches silent pipeline breaks by comparing the last hour's per-signal
fire counts against the 7-day baseline. If a previously-active signal
fires zero events in the last hour AND was averaging >5/hr historically,
something is broken — either a code regression, a database issue, or
the upstream wallet activity has truly dried up.

Also catches the inverse: a signal that suddenly fires 10x its baseline
is either a bug (filter regression) or a real event (resolution day,
news shock) and warrants attention either way.

Runs from the scheduler every 30 min. Sends ONE Telegram alert per
anomaly (dedup'd by signal_type within a 6h window).
"""
from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Anomaly thresholds — tuned to avoid spam on quiet hours.
SILENCE_MIN_BASELINE_PER_HOUR = 3.0   # ignore signals that average <3/hr
SILENCE_MULTIPLIER = 0.1              # alert if observed < 10% of baseline
SPIKE_MULTIPLIER = 5.0                # alert if observed > 5x baseline
DEDUP_HOURS = 6                       # don't repeat same anomaly within 6h


# Track recent anomalies in-process (cleared on container restart, fine).
_recent_alerts: dict[str, float] = {}


def _alert_recently_sent(key: str) -> bool:
    now = time.time()
    last = _recent_alerts.get(key, 0)
    if now - last < DEDUP_HOURS * 3600:
        return True
    _recent_alerts[key] = now
    return False


def detect_and_alert() -> dict:
    """One pass: compare last 1h vs 7d baseline per signal type."""
    from trading_platform.polymarket.db_connection import get_connection
    conn = get_connection()
    try:
        # Last hour fire counts per signal_type
        recent = dict(conn.execute("""
            SELECT signal_type, COUNT(*)
            FROM signal_outcomes
            WHERE fired_at > EXTRACT(EPOCH FROM NOW() - INTERVAL '1 hour')::bigint
            GROUP BY signal_type
        """).fetchall())

        # 7-day baseline: avg fires per hour, per signal type
        baselines = conn.execute("""
            SELECT signal_type, COUNT(*) / 168.0 AS hourly
            FROM signal_outcomes
            WHERE fired_at > EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days')::bigint
              AND fired_at < EXTRACT(EPOCH FROM NOW() - INTERVAL '1 hour')::bigint
            GROUP BY signal_type
        """).fetchall()
    finally:
        try: conn.close()
        except Exception: pass

    anomalies: list[dict] = []
    for sig, baseline_hourly in baselines:
        if baseline_hourly < SILENCE_MIN_BASELINE_PER_HOUR:
            continue  # too quiet historically to flag — noise floor
        observed = recent.get(sig, 0)
        ratio = observed / max(baseline_hourly, 0.001)

        if ratio < SILENCE_MULTIPLIER:
            anomalies.append({
                "type": "SILENT",
                "signal_type": sig,
                "observed_1h": observed,
                "baseline_hourly": round(baseline_hourly, 1),
                "ratio": round(ratio, 3),
            })
        elif ratio > SPIKE_MULTIPLIER:
            anomalies.append({
                "type": "SPIKE",
                "signal_type": sig,
                "observed_1h": observed,
                "baseline_hourly": round(baseline_hourly, 1),
                "ratio": round(ratio, 1),
            })

    # Dispatch alerts (dedup'd per signal_type)
    sent = 0
    if anomalies:
        try:
            from trading_platform.polymarket.telegram_alerts import get_alerter
            alerter = get_alerter()
            for a in anomalies:
                key = f"{a['type']}:{a['signal_type']}"
                if _alert_recently_sent(key):
                    continue
                emoji = "\U0001f507" if a["type"] == "SILENT" else "\U0001f4c8"
                msg = (
                    f"{emoji} <b>SIGNAL FLOW ANOMALY</b>\n"
                    f"<b>{a['type']}</b>: {a['signal_type']}\n"
                    f"Last 1h: {a['observed_1h']} fires "
                    f"(baseline {a['baseline_hourly']}/hr)\n"
                    f"Ratio: {a['ratio']}× baseline"
                )
                try:
                    if alerter._send(msg, disable_notification=True):
                        sent += 1
                except Exception:
                    pass
        except Exception as exc:
            logger.debug("alert dispatch failed: %s", exc)

    return {
        "checked_at": int(time.time()),
        "anomalies_found": len(anomalies),
        "alerts_sent": sent,
        "details": anomalies,
    }


def main() -> int:
    try:
        from trading_platform.polymarket.logging_config import setup_logging
        setup_logging(service="signal-anomaly")
    except Exception:
        logging.basicConfig(level=logging.INFO)
    result = detect_and_alert()
    logger.info(
        "[signal-anomaly] checked=%d found=%d sent=%d",
        result["checked_at"], result["anomalies_found"], result["alerts_sent"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
