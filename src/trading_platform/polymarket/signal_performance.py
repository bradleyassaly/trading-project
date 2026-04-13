"""
SignalPerformanceCalculator — EV, win rate, and live-readiness computation
over the ``signal_outcomes`` table.

This replaces the older `paper_analytics.signal_performance()` which read
from `polymarket_paper_trades` only. Reading from `signal_outcomes` gives
us *every* fired signal regardless of whether it became a paper trade,
which is what the Kelly sizer and kill switch need in order to measure
true edge rather than executor-filtered edge.
"""
from __future__ import annotations

import math
import sqlite3
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DB = _PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db"


def _one_sample_t_stat(values: list[float]) -> tuple[float, float]:
    """Return (t_stat, one_tailed_p_value) for H0: mean <= 0."""
    n = len(values)
    if n < 2:
        return 0.0, 1.0
    mean = sum(values) / n
    var = sum((x - mean) ** 2 for x in values) / (n - 1)
    if var <= 0:
        return (float("inf") if mean > 0 else 0.0), (0.0 if mean > 0 else 1.0)
    se = math.sqrt(var / n)
    t = mean / se if se else 0.0
    # Two-tailed p using survival of t distribution approximated as normal
    # (close enough at n>=20; conservative below).
    # p(T > t) ~ 0.5 * erfc(t / sqrt(2)) for one-tailed.
    p_one = 0.5 * math.erfc(t / math.sqrt(2)) if t > 0 else 1.0
    return t, p_one


class SignalPerformanceCalculator:
    """Computes edge, EV, and live-readiness from signal_outcomes."""

    # Live-readiness gates — conservative defaults.
    MIN_RESOLVED_FOR_LIVE = 20
    MIN_WIN_RATE = 0.52
    MAX_P_VALUE = 0.15
    MIN_POSITIVE_CATEGORIES = 2

    def __init__(self, db_path: str | Path | None = None) -> None:
        self._db_path = str(db_path or _DEFAULT_DB)

    def _fetch(self) -> list[dict[str, Any]]:
        conn = sqlite3.connect(self._db_path, timeout=60)
        try:
            cols = [
                "signal_type", "category", "direction", "confidence",
                "entry_price", "resolution_price", "outcome_delta",
                "is_win", "hold_days", "fired_at", "resolved_at",
                "paper_stake_usd", "paper_pnl",
            ]
            rows = conn.execute(
                f"SELECT {', '.join(cols)} FROM signal_outcomes"
            ).fetchall()
            return [dict(zip(cols, r)) for r in rows]
        finally:
            conn.close()

    def compute_all(self) -> dict[str, Any]:
        rows = self._fetch()

        # Partition
        by_type: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            by_type.setdefault(r["signal_type"] or "unknown", []).append(r)

        result_by_type: dict[str, Any] = {}
        for st, group in by_type.items():
            resolved = [r for r in group if r["resolution_price"] is not None]
            n_fired = len(group)
            n_open = n_fired - len(resolved)
            n_resolved = len(resolved)
            deltas = [r["outcome_delta"] for r in resolved if r["outcome_delta"] is not None]
            wins = sum(1 for r in resolved if r.get("is_win") == 1)

            wr = wins / n_resolved if n_resolved else 0.0
            avg_ev = sum(deltas) / len(deltas) if deltas else 0.0
            total_ev = sum(deltas) if deltas else 0.0
            t_stat, p_val = _one_sample_t_stat(deltas) if n_resolved >= 5 else (0.0, 1.0)

            # By category
            cat_groups: dict[str, list[dict[str, Any]]] = {}
            for r in resolved:
                cat_groups.setdefault(r["category"] or "unknown", []).append(r)
            by_cat: dict[str, dict[str, Any]] = {}
            for cat, cg in cat_groups.items():
                cg_deltas = [r["outcome_delta"] for r in cg if r["outcome_delta"] is not None]
                by_cat[cat] = {
                    "n": len(cg),
                    "wins": sum(1 for r in cg if r.get("is_win") == 1),
                    "win_rate": round(
                        sum(1 for r in cg if r.get("is_win") == 1) / len(cg), 4
                    ) if cg else 0,
                    "avg_ev": round(sum(cg_deltas) / len(cg_deltas), 4) if cg_deltas else 0,
                    "total_ev": round(sum(cg_deltas), 4) if cg_deltas else 0,
                }

            # By confidence bucket
            def bucket(conf: float | None) -> str:
                if conf is None:
                    return "unknown"
                if conf < 0.4:
                    return "low"
                if conf < 0.6:
                    return "medium"
                if conf < 0.8:
                    return "high"
                return "very_high"

            conf_groups: dict[str, list[dict[str, Any]]] = {}
            for r in resolved:
                conf_groups.setdefault(bucket(r["confidence"]), []).append(r)
            by_conf: dict[str, dict[str, Any]] = {}
            for b, cg in conf_groups.items():
                cg_deltas = [r["outcome_delta"] for r in cg if r["outcome_delta"] is not None]
                by_conf[b] = {
                    "n": len(cg),
                    "win_rate": round(
                        sum(1 for r in cg if r.get("is_win") == 1) / len(cg), 4
                    ) if cg else 0,
                    "avg_ev": round(sum(cg_deltas) / len(cg_deltas), 4) if cg_deltas else 0,
                }

            readiness = self._check_live_readiness(n_resolved, wr, avg_ev, p_val, by_cat)

            hold_days = [r["hold_days"] for r in resolved if r.get("hold_days") is not None]
            result_by_type[st] = {
                "total_fired": n_fired,
                "total_resolved": n_resolved,
                "total_open": n_open,
                "wins": wins,
                "losses": n_resolved - wins,
                "win_rate": round(wr, 4),
                "avg_ev": round(avg_ev, 4),
                "total_ev": round(total_ev, 4),
                "p_value": round(p_val, 4),
                "t_stat": round(t_stat, 3),
                "avg_hold_days": round(sum(hold_days) / len(hold_days), 2) if hold_days else None,
                "by_category": by_cat,
                "by_confidence": by_conf,
                "live_ready": readiness,
            }

        total_resolved = sum(v["total_resolved"] for v in result_by_type.values())
        total_wins = sum(v["wins"] for v in result_by_type.values())
        total_ev_sum = sum(v["total_ev"] for v in result_by_type.values())

        return {
            "by_type": result_by_type,
            "total_signals": len(rows),
            "total_resolved": total_resolved,
            "overall_win_rate": round(total_wins / total_resolved, 4) if total_resolved else None,
            "overall_total_ev": round(total_ev_sum, 4),
            "schema": "signal_outcomes",
        }

    def _check_live_readiness(
        self,
        n: int,
        wr: float,
        ev: float,
        p_val: float,
        by_cat: dict[str, Any],
    ) -> dict[str, Any]:
        positive_categories = sum(1 for c in by_cat.values() if c["avg_ev"] > 0)
        checks = {
            "sample_size": {
                "pass": n >= self.MIN_RESOLVED_FOR_LIVE,
                "value": n,
                "threshold": self.MIN_RESOLVED_FOR_LIVE,
            },
            "ev_positive": {"pass": ev > 0, "value": round(ev, 4)},
            "statistical_significance": {
                "pass": p_val < self.MAX_P_VALUE,
                "value": round(p_val, 4),
                "threshold": self.MAX_P_VALUE,
            },
            "win_rate": {
                "pass": wr > self.MIN_WIN_RATE,
                "value": round(wr, 4),
                "threshold": self.MIN_WIN_RATE,
            },
            "multi_category": {
                "pass": positive_categories >= self.MIN_POSITIVE_CATEGORIES,
                "value": positive_categories,
                "threshold": self.MIN_POSITIVE_CATEGORIES,
            },
        }
        checks["all_pass"] = all(c["pass"] for c in checks.values())
        return checks


def main() -> int:
    import json
    print(json.dumps(SignalPerformanceCalculator().compute_all(), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
