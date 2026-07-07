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
import os
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

# N4 (2026-07-07): champion/challenger holdout. Fit on the older train slice,
# gate persistence on the NEWEST tail (out-of-sample). An in-sample Brier
# always improves under isotonic regression, so the old in-sample gate
# persisted curves that memorized noise (the degenerate-collapse incidents).
# Clamped to a sane band.
HOLDOUT_FRAC = min(0.4, max(0.15, float(os.environ.get("CALIB_HOLDOUT_FRAC", "0.25"))))


_SCHEMA = """
CREATE TABLE IF NOT EXISTS alpha_calibration_curve (
    id              BIGSERIAL PRIMARY KEY,
    fitted_at       BIGINT NOT NULL,
    n_samples       BIGINT NOT NULL,
    breakpoints_json TEXT NOT NULL,
    brier_before    DOUBLE PRECISION,
    brier_after     DOUBLE PRECISION,
    window_days     BIGINT,
    category        TEXT
);
CREATE INDEX IF NOT EXISTS idx_acc_fitted_at ON alpha_calibration_curve(fitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_acc_category ON alpha_calibration_curve(category, fitted_at DESC);
"""


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            try:
                conn.execute(s)
            except Exception:
                pass
    # 2026-04-25: idempotent ALTER for the per-category column added to
    # an already-existing table. Postgres ADD COLUMN IF NOT EXISTS is
    # safe in concurrent workloads.
    try:
        conn.execute(
            "ALTER TABLE alpha_calibration_curve ADD COLUMN IF NOT EXISTS category TEXT"
        )
    except Exception:
        pass
    # 2026-05-25: per-slice calibration. Analyzer (calibration_per_slice.py)
    # showed Brier 0.58→0.26 on wallet_reversal SELL with a per-(signal × direction)
    # fit. apply_calibration() now prefers per-slice > per-category > global.
    # See [[project_wallet_system_gaps.md]] section "calibration".
    for col in ("signal_type", "direction"):
        try:
            conn.execute(
                f"ALTER TABLE alpha_calibration_curve ADD COLUMN IF NOT EXISTS {col} TEXT"
            )
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
    window_days: int = 30,
    category: str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    """Fit isotonic curve on resolved hypotheses; persist + return summary.

    If `category` is None, fits the GLOBAL curve. If category is set,
    fits a per-category curve (`category` column on the row). The
    apply_calibration() lookup falls back to the global curve when no
    per-category fit exists.
    """
    t0 = time.time()
    cutoff = int(time.time()) - window_days * 86400
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        _ensure_schema(conn)
        try:
            if category:
                rows = conn.execute(
                    """SELECT alpha_score, hypothesis_correct
                         FROM trade_hypotheses
                        WHERE resolved_at IS NOT NULL
                          AND resolved_at > ?
                          AND hypothesis_correct IN (0, 1)
                          AND alpha_score IS NOT NULL
                          AND category = ?
                        ORDER BY resolved_at ASC""",
                    (cutoff, category),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT alpha_score, hypothesis_correct
                         FROM trade_hypotheses
                        WHERE resolved_at IS NOT NULL
                          AND resolved_at > ?
                          AND hypothesis_correct IN (0, 1)
                          AND alpha_score IS NOT NULL
                        ORDER BY resolved_at ASC""",
                    (cutoff,),
                ).fetchall()
        except Exception as exc:
            return {"error": f"query: {exc}"}

        source = "trade_hypotheses"
        if len(rows) < 20:
            # 2026-07-06 fallback: the hypothesis-resolution loop starved
            # (18 resolved in 30d vs ~665 hypotheses created per week —
            # resolution enrichment was down with the gamma outage), so
            # the global curve went unfit for weeks while stale
            # breakpoints mapped every input to ~0.717. Resolved paper
            # trades carry the same (confidence, win/loss) information at
            # ~100x the volume (1,894 in the same 30d window) and are
            # already the ground truth the kill switch reads.
            try:
                # 2026-07-07 (audit C3/F2): train on confidence_RAW, not the
                # post-calibration/post-boost `confidence`. Fitting the curve
                # on its own boosted output is a closed feedback loop
                # (recursive shrinkage — the observable cause of Brier 0.32).
                # Pre-2026-07-07 rows lack confidence_raw and are excluded.
                _paper_sql = """SELECT confidence_raw,
                              CASE WHEN outcome = 'win' THEN 1 ELSE 0 END
                         FROM polymarket_paper_trades
                        WHERE exit_ts IS NOT NULL AND exit_ts > ?
                          AND outcome IN ('win', 'loss')
                          AND confidence_raw IS NOT NULL"""
                if category:
                    rows = conn.execute(
                        _paper_sql + " AND category = ? ORDER BY exit_ts ASC",
                        (cutoff, category),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        _paper_sql + " ORDER BY exit_ts ASC", (cutoff,)).fetchall()
                source = "polymarket_paper_trades"
            except Exception as exc:
                return {"error": f"paper fallback query: {exc}"}

        # N4: time-ordered champion/challenger. Fit on the older TRAIN slice,
        # judge on the NEWEST holdout tail. Require both partitions be big
        # enough for their Briers to mean anything.
        _n = len(rows)
        split_idx = _n - max(8, int(round(_n * HOLDOUT_FRAC)))
        train_rows = rows[:split_idx]
        holdout_rows = rows[split_idx:]
        if len(train_rows) < 20 or len(holdout_rows) < 8:
            return {
                "n": _n, "n_train": len(train_rows), "n_holdout": len(holdout_rows),
                "skipped": "need train>=20 and holdout>=8 for OOS gate",
                "source": source,
                "elapsed_seconds": round(time.time() - t0, 1),
            }

        xs_tr = [max(0.0, min(1.0, float(r[0]))) for r in train_rows]
        ys_tr = [int(r[1]) for r in train_rows]
        xs_ho = [max(0.0, min(1.0, float(r[0]))) for r in holdout_rows]
        ys_ho = [int(r[1]) for r in holdout_rows]

        # Fit on TRAIN only.
        breakpoints = isotonic_regression(xs_tr, ys_tr)
        bp_x = [b[0] for b in breakpoints]
        bp_y = [b[1] for b in breakpoints]

        def _apply(x: float) -> float:
            if not bp_x:
                return x
            i = bisect.bisect_right(bp_x, x) - 1
            if i < 0:
                return bp_y[0]
            return bp_y[i]

        # In-sample (train) Brier — persisted for back-compat/logging only.
        brier_before = sum((xs_tr[i] - ys_tr[i]) ** 2 for i in range(len(xs_tr))) / len(xs_tr)
        brier_after = sum((_apply(xs_tr[i]) - ys_tr[i]) ** 2 for i in range(len(xs_tr))) / len(xs_tr)

        # OUT-OF-SAMPLE (holdout) Briers — what the gate actually checks.
        # p_base is the TRAIN win-rate (a legitimate OOS base-rate null).
        p_base = sum(ys_tr) / len(ys_tr)
        brier_ho_before = sum((xs_ho[i] - ys_ho[i]) ** 2 for i in range(len(xs_ho))) / len(xs_ho)
        brier_ho_after = sum((_apply(xs_ho[i]) - ys_ho[i]) ** 2 for i in range(len(xs_ho))) / len(xs_ho)
        brier_ho_null = sum((p_base - ys_ho[i]) ** 2 for i in range(len(xs_ho))) / len(xs_ho)

        now = int(time.time())
        breakpoints_json = json.dumps(
            [{"x": round(x, 4), "y": round(y, 4)} for x, y in breakpoints]
        )
        # 2026-05-01: previously the only persistence guard was Brier
        # improvement >1%. Discovered today the live curve had collapsed
        # to a degenerate near-flat line (bp_y.max ≈ 0.50, all inputs
        # mapped into [0, 0.50]). That technically improves Brier on
        # balanced samples but destroys ALL signal — every gate that
        # needs confidence > 0.5 now rejects every trade. Add two
        # additional guards:
        #   1. n_samples >= 50 (prevents fitting on tiny noise samples)
        #   2. bp_y range >= 0.30 (prevents collapse to flat line)
        bp_y_vals = [b[1] for b in breakpoints]
        bp_range = max(bp_y_vals) - min(bp_y_vals) if bp_y_vals else 0
        bp_max = max(bp_y_vals) if bp_y_vals else 0
        too_flat = bp_range < 0.30
        # 2026-05-01: also reject curves where bp_max < 0.65. The first
        # calibration curve had bp_max=0.495 — high-conf signals could
        # never map above 50%, breaking every conf>0.5 gate downstream.
        too_compressed = bp_max < 0.65
        too_small = len(rows) < 50
        # N4: the primary gate is now OUT-OF-SAMPLE. The challenger (calibrated
        # curve) must beat both the raw confidence AND the base-rate null on
        # the held-out newest tail. An in-sample Brier always improves under
        # isotonic regression, so the old in-sample gate rubber-stamped
        # noise-memorizing curves.
        oos_improved = (brier_ho_after < brier_ho_before * 0.99
                        and brier_ho_after < brier_ho_null)
        if not oos_improved:
            logger.info(
                "[CALIB] %s curve not persisted (holdout Brier raw=%.4f null=%.4f "
                "cal=%.4f — no OOS improvement)",
                category or "GLOBAL", brier_ho_before, brier_ho_null, brier_ho_after,
            )
        elif too_flat:
            logger.warning(
                "[CALIB] %s curve REJECTED — degenerate flat line "
                "(bp_y range=%.2f < 0.30, max=%.2f). "
                "Low Brier from collapse, not calibration.",
                category or "GLOBAL", bp_range, bp_max,
            )
        elif too_compressed:
            logger.warning(
                "[CALIB] %s curve REJECTED — compressed top "
                "(bp_y max=%.2f < 0.65). Would kill every high-conf signal.",
                category or "GLOBAL", bp_max,
            )
        elif too_small:
            logger.info(
                "[CALIB] %s curve not persisted (n=%d < 50)",
                category or "GLOBAL", len(rows),
            )
        else:
            try:
                # Persist the HOLDOUT Briers (what the gate is on) so
                # downstream displays reflect out-of-sample quality.
                conn.execute(
                    """INSERT INTO alpha_calibration_curve
                         (fitted_at, n_samples, breakpoints_json,
                          brier_before, brier_after, window_days, category)
                       VALUES (?,?,?,?,?,?,?)""",
                    (now, len(train_rows), breakpoints_json,
                     round(brier_ho_before, 6), round(brier_ho_after, 6),
                     window_days, category),
                )
                conn.commit()
                logger.info(
                    "[CALIB] %s curve PERSISTED (holdout raw=%.4f null=%.4f cal=%.4f, "
                    "train=%d holdout=%d)",
                    category or "GLOBAL", brier_ho_before, brier_ho_null,
                    brier_ho_after, len(train_rows), len(holdout_rows),
                )
            except Exception as exc:
                logger.warning("calibration curve persist failed: %s", exc)

        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "n": len(rows),
            "n_train": len(train_rows),
            "n_holdout": len(holdout_rows),
            "source": source,
            "n_breakpoints": len(breakpoints),
            "brier_train_before": round(brier_before, 4),
            "brier_train_after": round(brier_after, 4),
            "brier_holdout_raw": round(brier_ho_before, 4),
            "brier_holdout_null": round(brier_ho_null, 4),
            "brier_holdout_cal": round(brier_ho_after, 4),
            "oos_improved": bool(brier_ho_after < brier_ho_before * 0.99
                                 and brier_ho_after < brier_ho_null),
            "breakpoints_json_preview": breakpoints_json[:300],
        }
    finally:
        try: conn.close()
        except Exception: pass


# ── Per-slice fit (2026-05-25) ─────────────────────────────────────────────
# The existing fit_calibration() above fits on `trade_hypotheses.alpha_score`
# (raw signal). The Brier-0.30-0.58 we measured was on the FINAL post-pipeline
# `confidence` value the executor actually saw. They diverge — fitting on
# alpha_score is mis-calibrating the wrong thing.
#
# fit_calibration_per_slice() below fits on (live_trades.confidence, outcome)
# pairs per (signal_type, direction). Persists with the new columns.
# apply_calibration() gains a feature-flag-gated lookup that prefers
# per-slice curves when ENABLE_PER_SLICE_CALIB=1.


def fit_calibration_per_slice(
    signal_type: str,
    direction: str,
    window_days: int = 60,
    db_path: str | None = None,
) -> dict[str, Any]:
    """Fit isotonic curve on LIVE outcomes per (signal × direction).

    Reads (confidence_raw, outcome) pairs from live_trades. 2026-07-07
    (audit C3/F2): trains on confidence_RAW — the pre-calibration,
    pre-boost signal confidence — not the post-pipeline `confidence`.
    Training on the post-pipeline value closed a feedback loop (the curve
    was fit on its own boosted output), and apply_calibration intercepts
    the RAW inbound confidence, so the x-axis must be raw for the curve's
    domain to match its input. Rows without confidence_raw (pre-2026-07-07)
    are excluded rather than back-contaminated.

    Persists to alpha_calibration_curve with signal_type + direction
    columns populated. apply_calibration() picks it up if
    ENABLE_PER_SLICE_CALIB=1.
    """
    t0 = time.time()
    cutoff = int(time.time()) - window_days * 86400
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        _ensure_schema(conn)
        try:
            rows = conn.execute(
                """SELECT confidence_raw,
                          CASE WHEN outcome='win' THEN 1
                               WHEN outcome='loss' THEN 0 END as y
                     FROM live_trades
                    WHERE dry_run=0 AND exit_ts >= ?
                      AND outcome IN ('win','loss')
                      AND confidence_raw IS NOT NULL
                      AND signal_type = ? AND direction = ?
                    ORDER BY exit_ts ASC""",
                (cutoff, signal_type, direction),
            ).fetchall()
        except Exception as exc:
            return {"error": f"query: {exc}"}

        # N4: time-ordered holdout, same as the global fit (smaller floors:
        # per-slice live volume is scarcer, so train>=15 / holdout>=6).
        _n = len(rows)
        split_idx = _n - max(6, int(round(_n * HOLDOUT_FRAC)))
        train_rows = rows[:split_idx]
        holdout_rows = rows[split_idx:]
        if len(train_rows) < 15 or len(holdout_rows) < 6:
            return {
                "signal_type": signal_type, "direction": direction,
                "n": _n, "n_train": len(train_rows), "n_holdout": len(holdout_rows),
                "skipped": "need train>=15 and holdout>=6",
            }

        xs_tr = [max(0.0, min(1.0, float(r[0]))) for r in train_rows]
        ys_tr = [int(r[1]) for r in train_rows]
        xs_ho = [max(0.0, min(1.0, float(r[0]))) for r in holdout_rows]
        ys_ho = [int(r[1]) for r in holdout_rows]

        breakpoints = isotonic_regression(xs_tr, ys_tr)
        bp_x = [b[0] for b in breakpoints]
        bp_y = [b[1] for b in breakpoints]

        def _apply(x: float) -> float:
            if not bp_x: return x
            i = bisect.bisect_right(bp_x, x) - 1
            return bp_y[0] if i < 0 else bp_y[i]

        brier_before = sum((xs_tr[i] - ys_tr[i]) ** 2 for i in range(len(xs_tr))) / len(xs_tr)
        brier_after = sum((_apply(xs_tr[i]) - ys_tr[i]) ** 2 for i in range(len(xs_tr))) / len(xs_tr)
        p_base = sum(ys_tr) / len(ys_tr)
        brier_ho_before = sum((xs_ho[i] - ys_ho[i]) ** 2 for i in range(len(xs_ho))) / len(xs_ho)
        brier_ho_after = sum((_apply(xs_ho[i]) - ys_ho[i]) ** 2 for i in range(len(xs_ho))) / len(xs_ho)
        brier_ho_null = sum((p_base - ys_ho[i]) ** 2 for i in range(len(xs_ho))) / len(xs_ho)

        # Guards — flat-curve on the train fit; persistence on OOS holdout.
        bp_range = (max(bp_y) - min(bp_y)) if bp_y else 0
        bp_max = max(bp_y) if bp_y else 0
        if bp_range < 0.15:
            return {"signal_type": signal_type, "direction": direction,
                    "n": _n, "skipped": f"flat curve (bp_range={bp_range:.2f})"}
        # Absolute holdout improvement (per-slice contract, not relative) AND
        # beat the base-rate null.
        if not (brier_ho_before - brier_ho_after > 0.005
                and brier_ho_after < brier_ho_null):
            return {"signal_type": signal_type, "direction": direction,
                    "n": _n, "n_train": len(train_rows), "n_holdout": len(holdout_rows),
                    "skipped": "no OOS holdout improvement",
                    "brier_holdout_raw": round(brier_ho_before, 4),
                    "brier_holdout_null": round(brier_ho_null, 4),
                    "brier_holdout_cal": round(brier_ho_after, 4)}

        now = int(time.time())
        breakpoints_json = json.dumps(
            [{"x": round(x, 4), "y": round(y, 4)} for x, y in breakpoints]
        )
        try:
            conn.execute(
                """INSERT INTO alpha_calibration_curve
                     (fitted_at, n_samples, breakpoints_json,
                      brier_before, brier_after, window_days,
                      category, signal_type, direction)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (now, len(train_rows), breakpoints_json,
                 round(brier_ho_before, 6), round(brier_ho_after, 6),
                 window_days, None, signal_type, direction),
            )
            conn.commit()
            logger.info("[CALIB][PER_SLICE] %s/%s PERSISTED holdout raw=%.3f "
                        "null=%.3f cal=%.3f (train=%d holdout=%d)",
                        signal_type, direction, brier_ho_before, brier_ho_null,
                        brier_ho_after, len(train_rows), len(holdout_rows))
        except Exception as exc:
            logger.warning("per-slice calibration persist failed: %s", exc)

        return {
            "signal_type": signal_type, "direction": direction,
            "n": _n, "n_train": len(train_rows), "n_holdout": len(holdout_rows),
            "brier_holdout_raw": round(brier_ho_before, 4),
            "brier_holdout_null": round(brier_ho_null, 4),
            "brier_holdout_cal": round(brier_ho_after, 4),
            "n_breakpoints": len(breakpoints),
            "elapsed_seconds": round(time.time() - t0, 1),
        }
    finally:
        try: conn.close()
        except Exception: pass


# ── Lookup API ──────────────────────────────────────────────────────────────

# Cache: keyed by category string ("" = global). Each entry is the
# bisect-ready (bp_x, bp_y) pair plus a fitted_at timestamp for TTL.
_curve_cache: dict[str, dict[str, Any]] = {}
_per_slice_cache: dict[tuple[str, str], dict[str, Any]] = {}
_curve_ttl_seconds = 300


def _load_per_slice_curve(signal_type: str, direction: str,
                          db_path: str | None = None) -> None:
    """Load the latest per-(signal × direction) curve into cache."""
    key = (signal_type, direction)
    now = time.time()
    cached = _per_slice_cache.get(key)
    if cached and now - cached.get("fitted_at", 0) < _curve_ttl_seconds:
        return
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        try:
            row = conn.execute(
                """SELECT breakpoints_json, fitted_at FROM alpha_calibration_curve
                    WHERE signal_type = ? AND direction = ?
                    ORDER BY fitted_at DESC LIMIT 1""",
                (signal_type, direction),
            ).fetchone()
        except Exception:
            row = None
        if not row:
            _per_slice_cache[key] = {"fitted_at": now, "db_fitted_at": 0, "bp_x": [], "bp_y": []}
            return
        try:
            bps = json.loads(row[0])
            _per_slice_cache[key] = {
                "fitted_at": now,
                "db_fitted_at": float(row[1] or 0),
                "bp_x": [float(b["x"]) for b in bps],
                "bp_y": [float(b["y"]) for b in bps],
            }
        except Exception:
            _per_slice_cache[key] = {"fitted_at": now, "db_fitted_at": 0, "bp_x": [], "bp_y": []}
    finally:
        try: conn.close()
        except Exception: pass


def _load_curve(category: str | None, db_path: str | None = None) -> None:
    global _curve_cache
    cache_key = category or ""
    now = time.time()
    cached = _curve_cache.get(cache_key)
    if cached and now - cached.get("fitted_at", 0) < _curve_ttl_seconds and cached.get("bp_x"):
        return
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        try:
            if category:
                row = conn.execute(
                    "SELECT breakpoints_json, fitted_at FROM alpha_calibration_curve "
                    "WHERE category = ? ORDER BY fitted_at DESC LIMIT 1",
                    (category,),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT breakpoints_json, fitted_at FROM alpha_calibration_curve "
                    "WHERE category IS NULL ORDER BY fitted_at DESC LIMIT 1"
                ).fetchone()
        except Exception:
            row = None
        if not row:
            _curve_cache[cache_key] = {"fitted_at": now, "bp_x": [], "bp_y": []}
            return
        try:
            bps = json.loads(row[0])
            bp_x = [float(b["x"]) for b in bps]
            bp_y = [float(b["y"]) for b in bps]
            # Keep the cache-refresh clock (`fitted_at`=now) separate from the
            # curve's real age (`db_fitted_at`). The old code overwrote the DB
            # timestamp with load time, so a curve that hadn't refit in weeks
            # always looked fresh and was served forever (audit finding 11 —
            # the "mapped every input to ~0.717 for weeks" incident).
            _curve_cache[cache_key] = {
                "fitted_at": now, "db_fitted_at": float(row[1] or 0),
                "bp_x": bp_x, "bp_y": bp_y,
            }
        except Exception as exc:
            logger.debug("curve parse failed: %s", exc)
            _curve_cache[cache_key] = {"fitted_at": now, "db_fitted_at": 0, "bp_x": [], "bp_y": []}
    finally:
        try: conn.close()
        except Exception: pass


def apply_calibration(
    alpha_score: float,
    category: str | None = None,
    db_path: str | None = None,
    signal_type: str | None = None,
    direction: str | None = None,
) -> float:
    """Map raw alpha_score through the latest isotonic curve.

    Lookup precedence (when ENABLE_PER_SLICE_CALIB=1):
        per-(signal × direction) → per-category → global
    Default (env unset): per-category → global (original behaviour).

    2026-05-25: per-slice is shadow-mode by default. Set
    ENABLE_PER_SLICE_CALIB=1 to swap Kelly's calibrated confidence to
    the per-slice curve. After 14d shadow proves no regression,
    flip the env to enable in production. Until then, the per-slice
    curves are fit + persisted but NOT used at trade time.
    """
    import os as _os
    if alpha_score is None:
        return 0.0
    try:
        x = max(0.0, min(1.0, float(alpha_score)))
    except (TypeError, ValueError):
        return 0.0

    # Max curve age at APPLY time. A curve that stopped refitting (its
    # source loop starved, or every recent refit failed the persist guard)
    # must not keep correcting live confidence forever — better to pass raw
    # than to apply a stale map. 21d > the slowest refit cadence (weekly)
    # with margin. Set MAX_CALIB_CURVE_AGE_DAYS to tune.
    _max_age = float(_os.environ.get("MAX_CALIB_CURVE_AGE_DAYS", "21")) * 86400
    _now = time.time()

    def _fresh(cache: dict) -> bool:
        age = _now - float(cache.get("db_fitted_at", 0) or 0)
        return cache.get("db_fitted_at", 0) and age <= _max_age

    # 1. Per-slice (only if feature flag set + signal info available)
    if (_os.environ.get("ENABLE_PER_SLICE_CALIB", "").lower() in ("1", "true", "yes")
            and signal_type and direction):
        _load_per_slice_curve(signal_type, direction, db_path)
        cached = _per_slice_cache.get((signal_type, direction)) or {}
        bp_x = cached.get("bp_x") or []
        bp_y = cached.get("bp_y") or []
        if bp_x and _fresh(cached):
            i = bisect.bisect_right(bp_x, x) - 1
            return bp_y[0] if i < 0 else bp_y[i]

    # 2. Per-category
    if category:
        _load_curve(category, db_path)
        cached = _curve_cache.get(category) or {}
        bp_x = cached.get("bp_x") or []
        bp_y = cached.get("bp_y") or []
        if bp_x and _fresh(cached):
            i = bisect.bisect_right(bp_x, x) - 1
            return bp_y[0] if i < 0 else bp_y[i]

    # 3. Global
    _load_curve(None, db_path)
    cached = _curve_cache.get("") or {}
    bp_x = cached.get("bp_x") or []
    bp_y = cached.get("bp_y") or []
    if not bp_x or not _fresh(cached):
        return x
    i = bisect.bisect_right(bp_x, x) - 1
    return bp_y[0] if i < 0 else bp_y[i]


def per_slice_curve_active(signal_type: str | None, direction: str | None,
                           db_path: str | None = None) -> bool:
    """True when per-slice calibration is enabled AND a non-empty per-(signal
    × direction) curve exists — i.e. apply_calibration() is already correcting
    this slice's confidence directly.

    Callers (the live executor) use this to avoid double-correcting: the
    sizing-multiplier band-aid should be SKIPPED for any slice the per-slice
    curve already calibrates, and kept only as a fallback for uncovered slices.
    """
    import os as _os
    if _os.environ.get("ENABLE_PER_SLICE_CALIB", "").lower() not in ("1", "true", "yes"):
        return False
    if not signal_type or not direction:
        return False
    try:
        _load_per_slice_curve(signal_type, direction, db_path)
        return bool((_per_slice_cache.get((signal_type, direction)) or {}).get("bp_x"))
    except Exception:
        return False


def main() -> int:
    """Fit global curve + per-category curves for any category with
    enough resolved data. Per-category curves take precedence at
    apply_calibration() lookup; categories without enough data fall
    back to the global curve."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # 1. Global fit
    global_result = fit_calibration()
    print({"scope": "global", **global_result})

    # 2. Per-category fits — only categories with enough resolved data
    #    will produce a curve; others get skipped. Reads the category
    #    list from trade_hypotheses.
    conn = get_connection()
    try:
        try:
            cat_rows = conn.execute(
                """SELECT category, COUNT(*) n
                     FROM trade_hypotheses
                    WHERE resolved_at IS NOT NULL
                      AND resolved_at > ?
                      AND hypothesis_correct IN (0, 1)
                      AND alpha_score IS NOT NULL
                      AND category IS NOT NULL
                    GROUP BY category
                   HAVING COUNT(*) >= 20""",
                (int(time.time()) - 30 * 86400,),
            ).fetchall()
        except Exception as exc:
            cat_rows = []
            print({"scope": "per_category", "error": str(exc)[:120]})
    finally:
        try: conn.close()
        except Exception: pass

    for cat, n in cat_rows:
        if not cat:
            continue
        result = fit_calibration(category=cat)
        print({"scope": "category", "category": cat, **result})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
