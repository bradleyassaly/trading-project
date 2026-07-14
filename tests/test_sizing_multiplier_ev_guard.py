"""EV guard on the position-size multiplier (post-mortem 2026-07-14).

compute_multiplier must never scale a net-losing slice ABOVE 1.0x, even when
WR > mean_confidence lifts the raw ratio over 1.0. Down-corrections
(over-confident slices) must still apply. The bug: resolution_decay got 1.49x
at actual_ev=-$0.36/trade, amplifying the bleed on the only live signal.
"""
import importlib.util
import os

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "update_sizing_multipliers",
    os.path.join(os.path.dirname(__file__), "..", "scripts",
                 "update_sizing_multipliers.py"),
)
usm = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(usm)
compute_multiplier = usm.compute_multiplier


def test_up_size_allowed_when_ev_positive():
    # WR 0.36 vs conf 0.24 -> raw 1.5; positive EV -> keep the up-size.
    assert compute_multiplier(0.36, 0.24, actual_ev=0.10) == pytest.approx(1.5)


def test_up_size_blocked_when_ev_nonpositive():
    # The resolution_decay case: raw ~1.49 but net-losing -> capped at 1.0.
    assert compute_multiplier(0.361, 0.243, actual_ev=-0.363) == pytest.approx(1.0)
    # exactly zero EV is also blocked (<= 0)
    assert compute_multiplier(0.36, 0.24, actual_ev=0.0) == pytest.approx(1.0)


def test_down_correction_unaffected_by_ev_guard():
    # Over-confident, net-losing slice still scales DOWN (guard only caps up).
    m = compute_multiplier(0.10, 0.30, actual_ev=-0.5)
    assert m == pytest.approx(0.10 / 0.30)
    assert m < 1.0


def test_ev_none_preserves_legacy_behavior():
    # No EV supplied -> unchanged raw behavior (backward compatible).
    assert compute_multiplier(0.36, 0.24, actual_ev=None) == pytest.approx(1.5)
    assert compute_multiplier(0.36, 0.24) == pytest.approx(1.5)


def test_clamps_and_zero_conf():
    assert compute_multiplier(0.9, 0.1, actual_ev=1.0) == pytest.approx(usm.MAX_MULT)  # 9x -> 2.0
    assert compute_multiplier(0.01, 0.9, actual_ev=1.0) == pytest.approx(usm.MIN_MULT)  # tiny -> 0.25
    assert compute_multiplier(0.5, 0.0, actual_ev=1.0) == pytest.approx(1.0)  # conf<=0 -> neutral
