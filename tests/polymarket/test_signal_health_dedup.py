"""N9: correlation auto-deprecation decision — kill the lower-IC member of a
correlated pair, never a protected signal, never on thin data or ties."""
from trading_platform.polymarket.signal_health import (
    _corr_dedup_decision, PROTECTED_SIGNALS, _DEDUP_MIN_N, _DEDUP_CORR_THRESHOLD,
)


def test_lower_ic_member_deprecated():
    dep, reason = _corr_dedup_decision(
        "cascade", ic_sig=0.05, n_sig=20, peer="convergence",
        peer_corr=0.85, ic_peer=0.20)
    assert dep == 1 and "convergence" in reason


def test_higher_ic_member_survives():
    dep, _ = _corr_dedup_decision(
        "convergence", ic_sig=0.20, n_sig=20, peer="cascade",
        peer_corr=0.85, ic_peer=0.05)
    assert dep == 0  # this member is the survivor


def test_protected_never_deprecated():
    # resolution_decay is the lower-IC member but is protected -> survives.
    assert "resolution_decay" in PROTECTED_SIGNALS
    dep, _ = _corr_dedup_decision(
        "resolution_decay", ic_sig=0.01, n_sig=50, peer="cascade",
        peer_corr=0.9, ic_peer=0.30)
    assert dep == 0


def test_thin_data_not_killed():
    dep, _ = _corr_dedup_decision(
        "cascade", ic_sig=0.05, n_sig=_DEDUP_MIN_N - 1, peer="convergence",
        peer_corr=0.9, ic_peer=0.30)
    assert dep == 0


def test_below_corr_threshold_ignored():
    dep, _ = _corr_dedup_decision(
        "cascade", ic_sig=0.05, n_sig=20, peer="convergence",
        peer_corr=_DEDUP_CORR_THRESHOLD - 0.01, ic_peer=0.30)
    assert dep == 0


def test_tie_ic_skipped():
    dep, _ = _corr_dedup_decision(
        "cascade", ic_sig=0.10, n_sig=20, peer="convergence",
        peer_corr=0.9, ic_peer=0.10)
    assert dep == 0  # tie -> neither killed


def test_corr_above_one_still_triggers():
    # correlation_max can exceed 1.0 (co-fire denominator under-count); must
    # not be clamped or error.
    dep, _ = _corr_dedup_decision(
        "cascade", ic_sig=0.05, n_sig=20, peer="convergence",
        peer_corr=1.857, ic_peer=0.30)
    assert dep == 1


def test_no_peer_noop():
    dep, _ = _corr_dedup_decision("cascade", 0.05, 20, "", 0.0, 0.0)
    assert dep == 0
