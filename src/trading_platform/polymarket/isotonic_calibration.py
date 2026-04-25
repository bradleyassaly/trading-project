"""Isotonic-regression calibration map for alpha_score → realized P(win).

Layer-4 deep fix. The first calibration run (Brier 0.35, miscal 0.31)
showed alpha_score is systematically overconfident: bin 0.4-0.5
predicts 0.43, realizes 0.20. Without correction, every downstream
sizing decision (Kelly, STAKE_MULTIPLIERS, behavioral boosts) is
multiplied against an inflated confidence.

Isotonic regression (Pool Adjacent Violators) fits a non-decreasing
step function p_calibrated = f(alpha_score) such that the empirical
mean(y) per bin matches the fitted curve. Unlike Platt scaling it
makes no parametric assumption — robust to skew, multimodal alpha
distributions, etc.

The fitted curve is persisted as a small JSON in
`alpha_calibration_curve` table (one row, snapshotted daily). Lookup
is `apply_calibration(alpha_score)` — O(log N) bisect into the
breakpoints, returning the calibrated probability.
"""
from __future__ import annotations

import bisect
import json
import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS alpha_calibration_curve (
    id              BIGSERIAL PRIMARY KEY,
    fitted_at       BIGINT NOT NULL,
    n_samples       BIGINT NOT NULL,
    breakpoints_json TEXT NOT NULL,
    brier_before    DOUBLE PRECISION,
    brier_after     DOUBLE PRECISION,
    window_days     BIGINT
);
CREATE INDEX IF NOT EXISTS idx_acc_fitted_at ON alpha_calibration_curve(fitted_at DESC);
"""


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            try:
                conn.execute(s)
            except Exception:
                pass


def isotonic_regression(
    xs: list[float], ys: list[float],
) -> list[tuple[float, float]]:
    """Pool Adjacent Violators algorithm.

    Returns a list of (x_breakpoint, y_value) pairs forming a
    non-decreasing step function. Lookup at any x: bisect, return
    y at preceding breakpoint.

    Algorithm: sort by x, then sweep merging adjacent blocks where
    y[i+1] < y[i] (PAV constraint violated), replacing both with
    weighted mean.
    """
    if len(xs) != len(ys) or not xs:
        return []
    paired = sorted(zip(xs, ys))
    blocks: list[dict[str, float]] = []
    for x, y in paired:
        blocks.append({"x_lo": x, "x_hi": x, "y": float(y), "w": 1.0})
        # Merge backwards while non-monotonic
        while len(blocks) >= 2 and blocks[-1]["y"] < blocks[-2]["y"]:
            a = blocks.pop()
            b = blocks.pop()
            new_w = a["w"] + b["w"]
            new_y = (a["y"] * a["w"] + b["y"] * b["w"]) / new_w
            blocks.append({"x_lo": b["x_lo"], "x_hi": a["x_hi"],
                           "y": new_y, "w": new_w})
    # Output as (x_lo, y) breakpoints
    return [(b["x_lo"], b["y"]) for b in blocks]


def fit_calibration(
    window_days: int = 30, db_path: str | None = None,
) -> dict[str, Any]:
    """Fit isotonic curve on resolved hypotheses; persist + return summary."""
    t0 = time.time()
    cutoff = int(time.time()) - window_days * 86400
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        _ensure_schema(conn)
        try:
            rows = conn.execute(
                """SELECT alpha_score, hypothesis_correct
                     FROM trade_hypotheses
                    WHERE resolved_at IS NOT NULL
                      AND resolved_at > ?
                      AND hypothesis_correct IN (0, 1)
                      AND alpha_score IS NOT NULL""",
                (cutoff,),
            ).fetchall()
        except Exception as exc:
            return {"error": f"query: {exc}"}

        if len(rows) < 20:
            return {
                "n": len(rows),
                "skipped": "need n>=20 for stable isotonic fit",
                "elapsed_seconds": round(time.time() - t0, 1),
            }

        xs = [max(0.0, min(1.0, float(r[0]))) for r in rows]
        ys = [int(r[1]) for r in rows]

        # Brier before
        brier_before = sum((xs[i] - ys[i]) ** 2 for i in range(len(xs))) / len(xs)

        breakpoints = isotonic_regression(xs, ys)

        # Brier after — apply curve to xs and recompute
        bp_x = [b[0] for b in breakpoints]
        bp_y = [b[1] for b in breakpoints]

        def _apply(x: float) -> float:
            if not bp_x:
                return x
            i = bisect.bisect_right(bp_x, x) - 1
            if i < 0:
                return bp_y[0]
            return bp_y[i]

        brier_after = sum(
            (_apply(xs[i]) - ys[i]) ** 2 for i in range(len(xs))
        ) / len(xs)

        now = int(time.time())
        breakpoints_json = json.dumps(
            [{"x": round(x, 4), "y": round(y, 4)} for x, y in breakpoints]
        )
        try:
            conn.execute(
                """INSERT INTO alpha_calibration_curve
                     (fitted_at, n_samples, breakpoints_json,
                      brier_before, brier_after, window_days)
                   VALUES (?,?,?,?,?,?)""",
                (now, len(rows), breakpoints_json,
                 round(brier_before, 6), round(brier_after, 6), window_days),
            )
            conn.commit()
        except Exception as exc:
            logger.warning("calibration curve persist failed: %s", exc)

        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "n": len(rows),
            "n_breakpoints": len(breakpoints),
            "brier_before": round(brier_before, 4),
            "brier_after": round(brier_after, 4),
            "improvement_pct": round(
                100 * (brier_before - brier_after) / max(brier_before, 1e-6), 1,
            ),
            "breakpoints_json_preview": breakpoints_json[:300],
        }
    finally:
        try: conn.close()
        except Exception: pass


# ── Lookup API ──────────────────────────────────────────────────────────────

_curve_cache: dict[str, Any] = {"fitted_at": 0, "bp_x": [], "bp_y": []}
_curve_ttl_seconds = 300


def _load_latest_curve(db_path: str | None = None) -> None:
    global _curve_cache
    now = time.time()
    if now - _curve_cache.get("fitted_at", 0) < _curve_ttl_seconds and _curve_cache["bp_x"]:
        return
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        try:
            row = conn.execute(
                "SELECT breakpoints_json, fitted_at FROM alpha_calibration_curve "
                "ORDER BY fitted_at DESC LIMIT 1"
            ).fetchone()
        except Exception:
            row = None
        if not row:
            _curve_cache = {"fitted_at": now, "bp_x": [], "bp_y": []}
            return
        try:
            bps = json.loads(row[0])
            bp_x = [float(b["x"]) for b in bps]
            bp_y = [float(b["y"]) for b in bps]
            _curve_cache = {"fitted_at": now, "bp_x": bp_x, "bp_y": bp_y}
        except Exception as exc:
            logger.debug("curve parse failed: %s", exc)
            _curve_cache = {"fitted_at": now, "bp_x": [], "bp_y": []}
    finally:
        try: conn.close()
        except Exception: pass


def apply_calibration(alpha_score: float, db_path: str | None = None) -> float:
    """Map raw alpha_score through the latest isotonic curve. Returns
    the calibrated probability. Falls back to identity on cold start.
    """
    if alpha_score is None:
        return 0.0
    try:
        x = max(0.0, min(1.0, float(alpha_score)))
    except (TypeError, ValueError):
        return 0.0
    _load_latest_curve(db_path)
    bp_x = _curve_cache.get("bp_x") or []
    bp_y = _curve_cache.get("bp_y") or []
    if not bp_x:
        return x
    i = bisect.bisect_right(bp_x, x) - 1
    if i < 0:
        return bp_y[0]
    return bp_y[i]


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(fit_calibration())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
