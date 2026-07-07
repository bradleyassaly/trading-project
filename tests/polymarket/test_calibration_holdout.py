"""N4: champion/challenger holdout gate for isotonic calibration.

The old in-sample gate persisted any curve (isotonic always lowers in-sample
Brier). The holdout gate must: (a) persist a genuinely calibratable signal,
(b) REJECT pure noise that an in-sample gate would have accepted.
"""
import os
import sqlite3
import tempfile

import pytest


def _seed(path, rows):
    """rows: list of (confidence_raw, outcome, exit_ts)."""
    c = sqlite3.connect(path)
    # empty trade_hypotheses -> forces the paper fallback source
    c.execute("CREATE TABLE trade_hypotheses (alpha_score REAL, hypothesis_correct INT, resolved_at INT, category TEXT)")
    c.execute("""CREATE TABLE polymarket_paper_trades (
                   confidence_raw REAL, outcome TEXT, exit_ts INT, category TEXT)""")
    c.execute("""CREATE TABLE alpha_calibration_curve (
                   fitted_at INT, n_samples INT, breakpoints_json TEXT,
                   brier_before REAL, brier_after REAL, window_days INT,
                   category TEXT, signal_type TEXT, direction TEXT)""")
    for cr, oc, ts in rows:
        c.execute("INSERT INTO polymarket_paper_trades (confidence_raw, outcome, exit_ts) VALUES (?,?,?)",
                  (cr, oc, ts))
    c.commit()
    c.close()


def _curves(path):
    c = sqlite3.connect(path)
    n = c.execute("SELECT COUNT(*) FROM alpha_calibration_curve").fetchone()[0]
    c.close()
    return n


def test_calibratable_signal_persists(monkeypatch):
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    # Systematically overconfident but monotone: higher confidence => higher
    # win rate, but shifted down. Isotonic recovers it and it holds OOS.
    # confidence_raw is CONTINUOUS and monotone but MIScalibrated: raw is
    # overconfident (true win-prob = raw**1.6), so isotonic adds real value.
    # Top of the range wins ~90% (clears the compression guard).
    rows = []
    ts = 1_783_000_000
    import random
    rnd = random.Random(7)
    for i in range(400):
        conf = round(rnd.uniform(0.10, 0.95), 3)
        true_p = conf ** 1.6
        win = "win" if rnd.random() < true_p else "loss"
        rows.append((conf, win, ts + i))
    _seed(path, rows)
    from trading_platform.polymarket import isotonic_calibration as ic
    out = ic.fit_calibration(window_days=3650, category=None, db_path=path)
    # honest monotone signal -> OOS improvement -> persists
    assert out.get("oos_improved") is True, out
    assert _curves(path) == 1
    os.unlink(path)


def test_pure_noise_rejected(monkeypatch):
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    # Confidence is unrelated to outcome (coin flip regardless of conf).
    # In-sample isotonic still lowers Brier; the holdout gate must reject it.
    rows = []
    ts = 1_783_000_000
    import random
    rnd = random.Random(11)
    for i in range(400):
        conf = round(rnd.uniform(0.10, 0.95), 3)
        win = "win" if rnd.random() < 0.5 else "loss"  # outcome independent of conf
        rows.append((conf, win, ts + i))
    _seed(path, rows)
    from trading_platform.polymarket import isotonic_calibration as ic
    out = ic.fit_calibration(window_days=3650, category=None, db_path=path)
    assert out.get("oos_improved") is not True, out
    assert _curves(path) == 0  # noise curve NOT persisted
    os.unlink(path)


def test_insufficient_holdout_skips(monkeypatch):
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    rows = [(0.5, "win", 1_783_000_000 + i) for i in range(10)]  # too few for a holdout
    _seed(path, rows)
    from trading_platform.polymarket import isotonic_calibration as ic
    out = ic.fit_calibration(window_days=3650, category=None, db_path=path)
    assert "skipped" in out
    assert _curves(path) == 0
    os.unlink(path)
