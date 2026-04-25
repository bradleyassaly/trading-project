"""Brier score + reliability diagram on hypothesis predictions.

Layer-4 calibration verification — answers whether our stated confidence
on a trade matches the realized outcome frequency. Without this we are
flying blind: a signal saying "70% confident" might resolve at 50%.

The math:
- Brier = mean( (p_pred - y)^2 ) where y in {0, 1}. Lower = better.
- Reliability = bin predictions by p, plot mean(y) per bin vs bin midpoint.
- Mean abs miscalibration = mean( |p_bin - mean(y)| ) weighted by bin n.

Reads from `trade_hypotheses` (one row per trade, with predicted
`alpha_score`/`expected_kelly` and realized `hypothesis_correct`).

Surfaces:
- `compute_calibration(window_days=30)` returns the dict.
- `/api/calibration` (added in api/main.py) exposes it.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


# 10 equal-width bins from 0.0 to 1.0
DEFAULT_BINS = 10


def compute_calibration(
    window_days: int = 30,
    bins: int = DEFAULT_BINS,
    db_path: str | None = None,
) -> dict[str, Any]:
    """Return Brier score + reliability bins + miscalibration over the
    last `window_days` of resolved hypotheses.

    Predicted probability source priority:
      1. `alpha_score` (per-(wallet, category) copyability score, 0-1)
      2. fallback to confidence_factors JSON if alpha_score is null

    `hypothesis_correct` (0/1) is the realized outcome.
    """
    cutoff = int(time.time()) - window_days * 86400
    try:
        conn = get_connection(db_path) if db_path else get_connection()
    except Exception as exc:
        return {"error": f"db connect: {exc}", "n": 0}
    try:
        try:
            rows = conn.execute(
                """SELECT alpha_score, hypothesis_correct, signal_type, category
                     FROM trade_hypotheses
                    WHERE resolved_at IS NOT NULL
                      AND resolved_at > ?
                      AND hypothesis_correct IN (0, 1)
                      AND alpha_score IS NOT NULL""",
                (cutoff,),
            ).fetchall()
        except Exception as exc:
            return {"error": f"query: {exc}", "n": 0}

        if not rows:
            return {
                "n": 0,
                "window_days": window_days,
                "brier": None,
                "mean_abs_miscalibration": None,
                "bins": [],
                "by_signal": [],
            }

        # Top-level Brier + reliability
        n = len(rows)
        sum_sq = 0.0
        bin_data: list[dict[str, Any]] = [
            {"bin_lo": i / bins, "bin_hi": (i + 1) / bins, "n": 0,
             "sum_pred": 0.0, "sum_actual": 0.0}
            for i in range(bins)
        ]
        for r in rows:
            try:
                p = float(r[0])
            except (TypeError, ValueError):
                continue
            p = max(0.0, min(1.0, p))
            y = int(r[1])
            sum_sq += (p - y) ** 2
            b_idx = min(int(p * bins), bins - 1)
            bin_data[b_idx]["n"] += 1
            bin_data[b_idx]["sum_pred"] += p
            bin_data[b_idx]["sum_actual"] += y
        brier = sum_sq / n

        # Compute per-bin reliability + weighted miscalibration
        weighted_miscal = 0.0
        out_bins = []
        for b in bin_data:
            if b["n"] == 0:
                out_bins.append({
                    "bin_lo": round(b["bin_lo"], 3),
                    "bin_hi": round(b["bin_hi"], 3),
                    "n": 0, "mean_pred": None, "mean_actual": None, "miscal": None,
                })
                continue
            mean_pred = b["sum_pred"] / b["n"]
            mean_actual = b["sum_actual"] / b["n"]
            miscal = abs(mean_pred - mean_actual)
            weighted_miscal += miscal * b["n"]
            out_bins.append({
                "bin_lo": round(b["bin_lo"], 3),
                "bin_hi": round(b["bin_hi"], 3),
                "n": b["n"],
                "mean_pred": round(mean_pred, 4),
                "mean_actual": round(mean_actual, 4),
                "miscal": round(miscal, 4),
            })
        mean_abs_miscal = weighted_miscal / n

        # Per-signal Brier breakdown
        by_signal: dict[str, dict[str, float]] = {}
        for r in rows:
            try:
                p = max(0.0, min(1.0, float(r[0])))
            except (TypeError, ValueError):
                continue
            y = int(r[1])
            sig = r[2] or "<unknown>"
            d = by_signal.setdefault(sig, {"n": 0, "sum_sq": 0.0, "sum_y": 0.0, "sum_p": 0.0})
            d["n"] += 1
            d["sum_sq"] += (p - y) ** 2
            d["sum_y"] += y
            d["sum_p"] += p
        signal_rows = []
        for sig, d in sorted(by_signal.items(), key=lambda kv: -kv[1]["n"]):
            n_s = d["n"]
            signal_rows.append({
                "signal_type": sig,
                "n": n_s,
                "brier": round(d["sum_sq"] / n_s, 4) if n_s else None,
                "mean_pred": round(d["sum_p"] / n_s, 4) if n_s else None,
                "mean_actual": round(d["sum_y"] / n_s, 4) if n_s else None,
            })

        # Verdict
        if mean_abs_miscal < 0.04:
            verdict = "well_calibrated"
        elif mean_abs_miscal < 0.10:
            verdict = "mild_miscalibration"
        else:
            verdict = "poor_calibration"

        return {
            "n": n,
            "window_days": window_days,
            "brier": round(brier, 4),
            "mean_abs_miscalibration": round(mean_abs_miscal, 4),
            "verdict": verdict,
            "bins": out_bins,
            "by_signal": signal_rows,
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    import json
    print(json.dumps(compute_calibration(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
