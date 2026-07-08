"""P1: canonical market_resolutions — monotonic precedence, conflicts,
token-space lookups. Resolution truth was scattered across 6 disagreeing
paths; this table is the single write-once-wins store they all feed."""
import os
import sqlite3
import tempfile

import pytest

from trading_platform.polymarket import resolutions as rs


@pytest.fixture()
def res_db(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    from trading_platform.polymarket import db_connection as dbc
    monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
    monkeypatch.setattr(rs, "_SCHEMA_READY", set())
    rs.ensure_schema(path)
    yield path
    os.unlink(path)


def test_insert_then_upgrade(res_db):
    assert rs.record_resolution("0xAA", source="gamma_bulk", resolves_yes=1,
                                db_path=res_db) == "inserted"
    # higher-rank source overwrites (even with a different value — it is
    # more trustworthy; that is what "upgrade" means)
    assert rs.record_resolution("0xaa", source="uma_gamma", resolves_yes=0,
                                db_path=res_db) == "upgraded"
    row = rs.get_resolution("0xAA", db_path=res_db)
    assert row["source"] == "uma_gamma" and row["resolves_yes"] == 0
    assert row["payout_yes"] == 0.0


def test_lower_rank_never_downgrades(res_db):
    rs.record_resolution("0xbb", source="uma_gamma", resolves_yes=1,
                         db_path=res_db)
    assert rs.record_resolution("0xbb", source="gamma_bulk", resolves_yes=1,
                                db_path=res_db) == "kept"
    assert rs.get_resolution("0xbb", db_path=res_db)["source"] == "uma_gamma"


def test_equal_rank_disagreement_logs_conflict(res_db):
    rs.record_resolution("0xcc", source="gamma_bulk", resolves_yes=1,
                         db_path=res_db)
    out = rs.record_resolution("0xcc", source="gamma_bulk", resolves_yes=0,
                               db_path=res_db)
    assert out == "conflict"
    # value unchanged; conflict row written
    assert rs.get_resolution("0xcc", db_path=res_db)["resolves_yes"] == 1
    c = sqlite3.connect(res_db)
    n = c.execute("SELECT COUNT(*) FROM market_resolution_conflicts "
                  "WHERE condition_id='0xcc'").fetchone()[0]
    c.close()
    assert n == 1


def test_lower_rank_disagreement_also_logs(res_db):
    rs.record_resolution("0xdd", source="uma_gamma", resolves_yes=1,
                         db_path=res_db)
    assert rs.record_resolution("0xdd", source="wallet_trades_vote",
                                resolves_yes=0, db_path=res_db) == "conflict"
    assert rs.get_resolution("0xdd", db_path=res_db)["resolves_yes"] == 1


def test_resolve_token_both_sides(res_db):
    # Regression for the ResolutionResolver inversion: a YES-token query on
    # a NO-won market must return False, and the NO token True.
    rs.record_resolution("0xee", source="uma_gamma", resolves_yes=0,
                         yes_token_id="tok_yes", no_token_id="tok_no",
                         db_path=res_db)
    assert rs.resolve_token("tok_yes", db_path=res_db) is False
    assert rs.resolve_token("tok_no", db_path=res_db) is True
    assert rs.resolve_token("unknown", db_path=res_db) is None


def test_non_binary_market(res_db):
    rs.record_resolution("0xff", source="uma_gamma", resolves_yes=None,
                         winning_outcome="Real Madrid", db_path=res_db)
    row = rs.get_resolution("0xff", db_path=res_db)
    assert row["resolves_yes"] is None
    assert row["winning_outcome"] == "Real Madrid"
    # token lookup refuses to guess on non-binary
    rs.record_resolution("0xff2", source="uma_gamma", resolves_yes=None,
                         yes_token_id="t1", no_token_id="t2", db_path=res_db)
    assert rs.resolve_token("t1", db_path=res_db) is None


def test_bulk_lookup(res_db):
    for i in range(3):
        rs.record_resolution(f"0x{i:02d}", source="gamma_bulk",
                             resolves_yes=i % 2, db_path=res_db)
    out = rs.get_resolutions_bulk(["0x00", "0x01", "0x99"], db_path=res_db)
    assert set(out) == {"0x00", "0x01"}
    assert out["0x01"]["payout_yes"] == 1.0


def test_unknown_source_rejected(res_db):
    with pytest.raises(ValueError):
        rs.record_resolution("0x11", source="vibes", resolves_yes=1,
                             db_path=res_db)


# ---------------------------------------------------------------------------
# Consumer cutover wiring (source-level regressions)
# ---------------------------------------------------------------------------

def test_consumers_cut_over_to_table():
    import inspect
    from trading_platform.polymarket import live_position_monitor as lpm
    from trading_platform.polymarket import polymarket_paper_executor as ppe
    from trading_platform.polymarket import enrich_resolution as er
    from trading_platform.polymarket import signal_resolver as sr
    # dust-close reads the canonical table before the CSV resolver
    lpm_src = inspect.getsource(lpm)
    assert "get_resolution" in lpm_src and "FALLBACK to CSV resolver" in lpm_src
    # paper settlement: bulk table read + majority vote; the LIMIT-1 row
    # lottery over contradictory wallet_trades is gone
    v2 = inspect.getsource(ppe.PolymarketPaperExecutor.check_resolutions_v2)
    assert "get_resolutions_bulk" in v2
    assert "AND market_outcome IS NOT NULL LIMIT 1" not in v2  # the old lottery SQL
    assert "contradictory" in v2
    # enrich writes Polymarket's own settlement into the table
    assert 'source="data_api_positions"' in inspect.getsource(er)
    # resolver: Pass 0 table read + UMA upgrade pass + uma_gamma writes
    sr_src = inspect.getsource(sr)
    assert "get_resolutions_bulk" in sr_src
    assert 'source="uma_gamma"' in sr_src
    assert "upgrade pass" in sr_src


def test_token_ids_survive_upgrade_without_them(res_db):
    # gamma_bulk knew the token ids; a later uma_gamma upgrade without them
    # must not NULL them out (COALESCE semantics).
    rs.record_resolution("0x22", source="gamma_bulk", resolves_yes=1,
                         yes_token_id="ty", no_token_id="tn", db_path=res_db)
    rs.record_resolution("0x22", source="uma_gamma", resolves_yes=1,
                         db_path=res_db)
    row = rs.get_resolution("0x22", db_path=res_db)
    assert row["yes_token_id"] == "ty" and row["no_token_id"] == "tn"
    assert row["source"] == "uma_gamma"
