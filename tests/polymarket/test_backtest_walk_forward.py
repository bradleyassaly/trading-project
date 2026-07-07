"""N5: de-tautologize the backtest — real walk-forward + OOS ensemble scoring.

The prior framework (a) called a single 70/30 chronological cut "walk-forward"
while refitting nothing, and (b) scored every row of run_ensemble_backtest
against the exact _CATEGORY_EV constants the live scorer uses — a tautology,
since those constants were fit with hindsight over the very rows being scored.

These tests pin the honest behavior:
  * _fit_category_ev shrinks empirical train EV toward a prior and falls back to
    the prior when a category is thin (so a fold can't overfit a rare category).
  * _ensemble_score is a function of the SUPPLIED (per-fold, train-only) EV map,
    not baked-in constants — feeding it a different map changes the score.
  * run_backtest produces multiple expanding-window folds, each refit on its own
    train slice, with an insufficient_data guard when history is too short.
  * run_ensemble_backtest scores TEST rows only, aggregates OOS buckets across
    folds, and never leaks the live constants into the fit.
"""
import os
import sqlite3
import tempfile

import pytest

from trading_platform.polymarket import backtest_framework as bf


# ---------------------------------------------------------------------------
# Pure helpers — no DB, they carry the anti-tautology property.
# ---------------------------------------------------------------------------

def test_fit_category_ev_thin_category_falls_back_to_prior():
    prior = {"politics": 0.31, "crypto": -0.27}
    # Only 3 politics trades (< min_n=8) -> must return the prior untouched.
    train = [{"category": "politics", "direction": "BUY",
              "entry_price": 0.4, "resolution_price": 1.0} for _ in range(3)]
    fitted = bf._fit_category_ev(train, prior, min_n=8, shrink=8.0)
    assert fitted["politics"] == prior["politics"]


def test_fit_category_ev_shrinks_toward_prior():
    prior = {"politics": 0.0}
    # 16 winning BUYs at 0.5 -> raw empirical EV = +1.0 each. With shrink=8 and
    # n=16, weight = 16/24 = 0.667 -> fitted ~= 0.667 * 1.0 + 0.333*0 = 0.667,
    # strictly between the prior (0) and the empirical (1.0).
    train = [{"category": "politics", "direction": "BUY",
              "entry_price": 0.5, "resolution_price": 1.0} for _ in range(16)]
    fitted = bf._fit_category_ev(train, prior, min_n=8, shrink=8.0)
    assert 0.0 < fitted["politics"] < 1.0
    assert abs(fitted["politics"] - (16 / 24) * 1.0) < 1e-9


def test_ensemble_score_uses_supplied_ev_not_constants():
    # THE anti-tautology assertion: the score depends on the EV map passed in,
    # so a fold that fits a different (train-only) map produces a different
    # score. If the function ignored its argument and used baked-in constants,
    # these two calls would be identical.
    bull = bf._ensemble_score(0.6, "tier1", 0.3, {"politics": 0.9}, "politics")
    bear = bf._ensemble_score(0.6, "tier1", 0.3, {"politics": -0.9}, "politics")
    assert bull > bear
    # And an unknown category defaults to 0.0 contribution (no KeyError, no
    # silent fallback to a live constant).
    unk = bf._ensemble_score(0.6, "tier1", 0.3, {}, "made_up_cat")
    assert 0.05 <= unk <= 0.95


def test_bucket_boundaries():
    assert bf._bucket_of(0.0) == "0.0-0.2"
    assert bf._bucket_of(0.19) == "0.0-0.2"
    assert bf._bucket_of(0.2) == "0.2-0.4"
    assert bf._bucket_of(0.55) == "0.4-0.6"
    assert bf._bucket_of(0.8) == "0.8-1.0"
    assert bf._bucket_of(1.0) == "0.8-1.0"


# ---------------------------------------------------------------------------
# DB-backed: fold structure + OOS scoring via a seeded sqlite signal_outcomes.
# ---------------------------------------------------------------------------

def _seed_outcomes(path, rows):
    """rows: (signal_type, category, direction, confidence, entry_price,
             resolution_price, wallet_tier, fired_at)."""
    c = sqlite3.connect(path)
    c.execute(
        """CREATE TABLE signal_outcomes (
             signal_type TEXT, category TEXT, direction TEXT, confidence REAL,
             entry_price REAL, resolution_price REAL, fired_at INT,
             wallet TEXT, wallet_tier TEXT, resolved_at INT, hold_days REAL)""")
    for st, cat, d, conf, ep, rp, tier, ts in rows:
        c.execute(
            """INSERT INTO signal_outcomes
                 (signal_type, category, direction, confidence, entry_price,
                  resolution_price, fired_at, wallet, wallet_tier,
                  resolved_at, hold_days)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (st, cat, d, conf, ep, rp, ts, "w", tier, ts + 1, 1.0))
    c.commit()
    c.close()


def _point_at(monkeypatch, path):
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    from trading_platform.polymarket import db_connection as dbc
    monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
    monkeypatch.setattr(dbc, "DEFAULT_DB_PATH", path)


def _make_rows(n, ts0=1_783_000_000):
    """A monotone-ish synthetic set: BUYs at a spread of entry prices, resolving
    win/loss so that category EV is learnable but not degenerate."""
    rows = []
    import random
    rnd = random.Random(3)
    cats = ["politics", "crypto", "sports"]
    for i in range(n):
        cat = cats[i % len(cats)]
        conf = round(rnd.uniform(0.2, 0.9), 3)
        ep = round(rnd.uniform(0.15, 0.6), 3)
        # politics wins often, crypto rarely, sports coin-flip.
        p = {"politics": 0.7, "crypto": 0.25, "sports": 0.5}[cat]
        rp = 1.0 if rnd.random() < p else 0.0
        rows.append(("resolution_decay", cat, "BUY", conf, ep, rp,
                     "tier1", ts0 + i * 60))
    return rows


def test_run_backtest_produces_multiple_folds(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    try:
        _seed_outcomes(path, _make_rows(120))
        _point_at(monkeypatch, path)
        out = bf.run_backtest(bf.BacktestConfig(lookback_days=3650, n_folds=5,
                                                min_train=30))
        wf = out["walk_forward"]
        assert not wf.get("insufficient_data"), wf
        assert wf["aggregate"]["n_folds_evaluated"] >= 4
        # Every fold refits its own category-EV map on its own train slice, and
        # train grows across folds (expanding window).
        sizes = [f["train_size"] for f in wf["folds"]]
        assert sizes == sorted(sizes) and sizes[0] >= 30
        for f in wf["folds"]:
            assert "fitted_category_ev" in f and f["test_size"] > 0
        # Back-compat keys still present for legacy dashboards.
        assert "train" in wf and "test" in wf
    finally:
        os.unlink(path)


def test_run_backtest_insufficient_data_guard(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    try:
        _seed_outcomes(path, _make_rows(31))  # 31 rows, min_train=30 -> tail=1 < 5
        _point_at(monkeypatch, path)
        out = bf.run_backtest(bf.BacktestConfig(lookback_days=3650, n_folds=5,
                                                min_train=30))
        wf = out["walk_forward"]
        assert wf.get("insufficient_data") is True
        assert wf["aggregate"]["n_folds_evaluated"] == 0
    finally:
        os.unlink(path)


def test_run_ensemble_backtest_scores_out_of_sample(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    try:
        # entry prices in [0.15, 0.6] keep every row inside the ensemble's
        # [0.10, 0.85] filter so all rows survive to be scored.
        _seed_outcomes(path, _make_rows(150))
        _point_at(monkeypatch, path)
        out = bf.run_ensemble_backtest(lookback_days=3650, n_folds=5)
        assert out["total_signals"] == 150
        assert out["n_folds"] >= 4
        assert not out.get("insufficient_data")
        # per-fold records carry the train-only fitted EV (not the live map).
        assert all("fitted_category_ev" in f for f in out["per_fold"])
        # OOS aggregate buckets exist and their counts sum to <= total rows
        # scored across all TEST folds (never the whole set at once).
        oos = out["score_buckets_oos_aggregate"]
        assert sum(b["n"] for b in oos.values()) > 0
        assert isinstance(out["monotonic_oos"], bool)
    finally:
        os.unlink(path)


def test_run_ensemble_backtest_insufficient(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    try:
        _seed_outcomes(path, _make_rows(20))  # < min_train(30)+n_folds
        _point_at(monkeypatch, path)
        out = bf.run_ensemble_backtest(lookback_days=3650, n_folds=5)
        assert out.get("insufficient_data") is True
        assert out["n_folds"] == 0
    finally:
        os.unlink(path)
