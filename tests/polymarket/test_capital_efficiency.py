"""N2: capital-efficiency metric arithmetic + review-section robustness."""
import sqlite3


def test_deployed_and_idle_partition_equity():
    # deployed_pct + idle_usdc/total_equity == 1 by construction.
    usdc, open_mkt = 279.44, 35.75
    total_equity = usdc + open_mkt
    idle_usdc = usdc
    deployed_pct = open_mkt / total_equity
    assert abs(deployed_pct + idle_usdc / total_equity - 1.0) < 1e-9


def test_unredeemed_only_counts_resolved_held():
    # Only positions with a known payout AND a nonzero on-chain balance count;
    # losers (payout 0) contribute 0.
    positions = [
        # (shares, payout_or_None, on_chain)
        (100, 1.0, 100),   # resolved winner, still held -> 100
        (50, 0.0, 50),     # resolved loser, still held -> 0
        (30, None, 30),    # unresolved -> not counted
        (40, 1.0, 0),      # resolved+winner but already redeemed -> not held
    ]
    unredeemed = sum(sh * pay for sh, pay, oc in positions
                     if pay is not None and oc > 0)
    assert unredeemed == 100.0


def test_review_section_handles_null_columns(monkeypatch):
    # Old pre-migration snapshot rows have NULL metric columns — section must
    # coalesce, not crash.
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "dsr", "scripts/daily_system_review.py")
    dsr = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dsr)
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE live_equity_snapshots (
        ts INT, total_equity REAL, deployed_pct REAL, idle_usdc REAL,
        unredeemed_value REAL)""")
    conn.execute("INSERT INTO live_equity_snapshots VALUES (1, 300, NULL, NULL, NULL)")
    conn.commit()
    dsr.section_capital_efficiency(conn, 1)  # must not raise
    conn.close()
