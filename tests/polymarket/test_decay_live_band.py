"""S1 (2026-07-27): live-lane entry band for resolution_decay BUYs.

The post-band-fix live cohort ran EV/$ −0.127 with the loss migrated into
the 0.30-0.40 band opened by the 2026-04-27 widening. Live entries are
restricted to [0.05, 0.20] (the only band with a measured positive prior)
and esports subcategories are excluded; paper keeps the full aperture.
The gate is pure (px, subcategory) → reason|None and runs BEFORE probe
tagging, so $1 probes obey it too.
"""
import pytest

from trading_platform.polymarket import polymarket_live_executor as plx


def test_in_band_passes():
    assert plx.decay_live_entry_block(0.12, None) is None
    assert plx.decay_live_entry_block(0.05, "sports/soccer") is None
    assert plx.decay_live_entry_block(0.20, "sports") is None


def test_migrated_band_blocked():
    # The 0.30-0.40 band that absorbed the post-fix bleed (25/29 fires).
    why = plx.decay_live_entry_block(0.34, "sports")
    assert why and "outside" in why


def test_old_exclusion_band_blocked_at_execution():
    # Fire→fill drift let 3 post-fix entries land inside 0.20-0.30 even
    # though signal-time exclusion was active. The executor-side check
    # closes that leak.
    why = plx.decay_live_entry_block(0.25, None)
    assert why and "outside" in why


def test_below_floor_blocked():
    assert plx.decay_live_entry_block(0.03, None)


def test_esports_excluded_any_shape():
    # markets.subcategory arrives in several shapes; all must match.
    for sub in ("esports", "sports/esports", "esports/lol", "Esports"):
        why = plx.decay_live_entry_block(0.12, sub)
        assert why and "subcat" in why, sub


def test_soccer_not_excluded_by_substring():
    # 'esports' must not match by substring inside other tokens.
    assert plx.decay_live_entry_block(0.12, "sports/soccer") is None
    assert plx.decay_live_entry_block(0.12, "sports") is None


def test_no_price_basis_passes_to_downstream_gates():
    # px<=0 → the calibrated-EV gate owns the decision; subcat still binds.
    assert plx.decay_live_entry_block(0.0, "sports") is None
    assert plx.decay_live_entry_block(0.0, "esports")


def test_env_overrides(monkeypatch):
    monkeypatch.setattr(plx, "RD_LIVE_ENTRY_HIGH", 0.40)
    assert plx.decay_live_entry_block(0.34, "sports") is None
    monkeypatch.setattr(plx, "RD_LIVE_EXCLUDE_SUBCATS", frozenset({"tennis"}))
    assert plx.decay_live_entry_block(0.12, "sports/tennis")
    assert plx.decay_live_entry_block(0.12, "esports/lol") is None
