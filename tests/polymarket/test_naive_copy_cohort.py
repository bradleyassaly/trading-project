"""N8: the naive-copy cohort must exclude wash/sybil-flagged wallets.

First test to exercise the real _get_cohort. Uses in-memory sqlite; _get_cohort
takes a conn arg so no DB-backend dependency.
"""
import sqlite3

from trading_platform.polymarket import naive_copy_signal


def _seed(conn):
    conn.execute(
        """CREATE TABLE wallet_profiles (
             wallet TEXT PRIMARY KEY, wq_score REAL, wq_n_60d INTEGER,
             wq_wr_60d REAL, wq_recency_days INTEGER)"""
    )
    conn.execute(
        """CREATE TABLE wallet_behavior_metrics (
             wallet TEXT PRIMARY KEY, is_likely_farmer INTEGER, sybil_score REAL)"""
    )
    # clean, high quality, no behavior row -> eligible
    conn.execute("INSERT INTO wallet_profiles VALUES ('clean', 0.9, 100, 0.70, 5)")
    # farmer-flagged -> excluded
    conn.execute("INSERT INTO wallet_profiles VALUES ('farmer', 0.95, 200, 0.80, 2)")
    conn.execute("INSERT INTO wallet_behavior_metrics VALUES ('farmer', 1, 0.9)")
    # high sybil_score but not binary-flagged -> excluded by the score predicate
    conn.execute("INSERT INTO wallet_profiles VALUES ('borderline', 0.92, 150, 0.75, 3)")
    conn.execute("INSERT INTO wallet_behavior_metrics VALUES ('borderline', 0, 0.7)")
    # clean with a benign behavior row -> eligible
    conn.execute("INSERT INTO wallet_profiles VALUES ('clean2', 0.8, 60, 0.60, 10)")
    conn.execute("INSERT INTO wallet_behavior_metrics VALUES ('clean2', 0, 0.1)")
    conn.commit()


def test_cohort_excludes_farmers_and_high_sybil():
    conn = sqlite3.connect(":memory:")
    _seed(conn)
    cohort = set(naive_copy_signal._get_cohort(conn))
    assert "clean" in cohort           # no wbm row -> allowed
    assert "clean2" in cohort          # benign wbm row -> allowed
    assert "farmer" not in cohort      # is_likely_farmer=1 -> excluded
    assert "borderline" not in cohort  # sybil_score 0.7 >= 0.6 -> excluded
    conn.close()
