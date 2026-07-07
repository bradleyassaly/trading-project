"""C3: copyable-CI gate (shadow-first) — is_copyable_ci finally read at trade time.

is_copyable_ci (roi_lower_95 > 0 AND n >= 20) is the strongest
anti-survivorship metric in the codebase; until this gate the live executor
never read it. Shadow by default (COPYABLE_CI_ENFORCE=0): traces pass/fail
for the promotion review, never blocks. These tests pin the pure verdict and
the executor wiring (source-level, both surfaces).
"""
import inspect

from trading_platform.polymarket.wallet_behavior_metrics import copyable_ci_verdict


WALLET = "0x" + "a" * 40


def test_copyable_passes():
    ok, why = copyable_ci_verdict((1, 45, 0.12), WALLET)
    assert ok is True
    assert "n=45" in why and "+0.120" in why


def test_not_copyable_fails():
    # Real case from recon: 0xc8ab97... n=1414 roi_lb=-0.123 — the gate's
    # whole point is that this wallet keeps getting copied live.
    ok, why = copyable_ci_verdict((0, 1414, -0.123), WALLET)
    assert ok is False
    assert "-0.123" in why


def test_missing_metrics_row_fails():
    # An unmeasured wallet is exactly what an anti-survivorship gate must
    # not copy: no row = fail, not fail-open.
    ok, why = copyable_ci_verdict(None, WALLET)
    assert ok is False and why == "no_metrics"


def test_truncated_wallet_flagged_distinctly():
    # live_trades.signal_wallet holds truncated ids like '0x507e52ef68'
    # (12 chars) — they can never join wallet_behavior_metrics, and must not
    # be counted as no_metrics in the shadow stats.
    ok, why = copyable_ci_verdict(None, "0x507e52ef68")
    assert ok is False and why == "truncated_wallet"
    ok, why = copyable_ci_verdict((1, 50, 0.2), "")
    assert ok is False and why == "truncated_wallet"


def test_null_roi_lower_handled():
    ok, why = copyable_ci_verdict((0, 3, None), WALLET)
    assert ok is False and "+0.000" in why


# ---------------------------------------------------------------------------
# Executor wiring (source-level regression — both surfaces, correct columns)
# ---------------------------------------------------------------------------

def test_live_executor_wired_shadow_default():
    from trading_platform.polymarket import polymarket_live_executor as ple
    src = inspect.getsource(ple)
    assert "is_copyable_ci" in src
    assert "COPYABLE_CI_ENFORCE" in src
    assert "LIVE_COPYABLE_CI_SHADOW" in src
    # enforcement must default OFF (shadow) pending the N6/C1+C2 verdict
    assert '"COPYABLE_CI_ENFORCE", "0"' in src


def test_paper_executor_wired_shadow_only():
    from trading_platform.polymarket import polymarket_paper_executor as ppe
    src = inspect.getsource(ppe)
    assert "COPYABLE_CI_SHADOW" in src
    assert "copyable_ci_verdict" in src
    # paper surface must never enforce (no block on verdict fail)
    assert "COPYABLE_CI_ENFORCE" not in src
