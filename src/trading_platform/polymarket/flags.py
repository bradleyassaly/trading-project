"""Centralized feature flags + tunable constants.

Single source of truth for everything previously scattered across
docker-compose.yml, .env, code constants, and DB rows. Each flag has:
  * the env var name
  * the default
  * a brief comment on intent
  * a typed getter

Importers should call the getter once at module load (or per use for
hot-reloadable flags) rather than re-reading os.environ each call.

NOTE: not all 59 environment variables found in the codebase are
migrated here yet. Phase 4.2 migration ports the ones we're actively
toggling for the alpha-generation experiments. Others (auth keys,
infrastructure tunables) can stay as direct env reads where they live.
"""
from __future__ import annotations

import os


def _bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


# ── Strategy lanes ────────────────────────────────────────────────────────
# Whale-derived signals (the original lane). Default ON.
LIVE_TRADING_ENABLED = _bool("POLYMARKET_LIVE_ENABLED", True)
LIVE_DISCOVERY_TIER = _bool("LIVE_DISCOVERY_ENABLED", True)

# Phase B independent signal — resolution-time decay (orthogonal to whales).
PHASE_B_RESOLUTION_DECAY = _bool("PHASE_B_RESOLUTION_DECAY_ENABLED", False)

# Naive-copy lane (2026-06-03). Default shadow.
NAIVE_COPY_LIVE = _bool("NAIVE_COPY_LIVE_ENABLED", False)
NAIVE_COPY_MAX_OPEN = _int("NAIVE_COPY_MAX_OPEN", 10)
NAIVE_COPY_STAKE_USD = _float("NAIVE_COPY_STAKE_USD", 5.0)
NAIVE_COPY_LOOKBACK_MIN = _int("NAIVE_COPY_LOOKBACK_MIN", 75)
NAIVE_COPY_PER_CYCLE_CAP = _int("NAIVE_COPY_PER_CYCLE_CAP", 5)


# ── Calibration / sizing ─────────────────────────────────────────────────
# Per-(signal_type × direction) isotonic calibration. Replaces global curve
# when set. Validated 2026-06-02; Brier 0.58→0.18 on top SELL signals.
PER_SLICE_CALIB = _bool("ENABLE_PER_SLICE_CALIB", False)


# ── Portfolio risk (Phase 3) ─────────────────────────────────────────────
# Volatility-target stake sizer. Off by default (shadow). When on, scales
# stake by target_daily_loss / realized_vol_7d.
VOL_TARGET = _bool("ENABLE_VOL_TARGET", False)
VOL_TARGET_FRAC = _float("VOL_TARGET_FRAC", 0.03)  # 3% of equity / day
VOL_TARGET_MIN_SCALE = _float("VOL_TARGET_MIN_SCALE", 0.5)
VOL_TARGET_MAX_SCALE = _float("VOL_TARGET_MAX_SCALE", 1.5)

# Forward-looking VaR gate. Off by default; when on, blocks new entries
# if 30d VaR(5%) exceeds VAR_HARD_CAP_FRAC of equity.
VAR_GATE = _bool("ENABLE_VAR_GATE", False)
VAR_HARD_CAP_FRAC = _float("VAR_HARD_CAP_FRAC", 0.10)


# ── Order book monitor ───────────────────────────────────────────────────
OB_MONITOR = _bool("POLYMARKET_ENABLE_OB_MONITOR", False)
HOT_SCANNER = _bool("POLYMARKET_ENABLE_HOT_SCANNER", False)


# ── Bankroll / sizing ────────────────────────────────────────────────────
LIVE_BANKROLL_USD = _float("POLYMARKET_LIVE_BANKROLL_USD", 0.0)


def snapshot() -> dict:
    """Return current flag values for logging at startup."""
    return {
        "live_trading": LIVE_TRADING_ENABLED,
        "live_discovery": LIVE_DISCOVERY_TIER,
        "phase_b_resolution_decay": PHASE_B_RESOLUTION_DECAY,
        "naive_copy_live": NAIVE_COPY_LIVE,
        "naive_copy_max_open": NAIVE_COPY_MAX_OPEN,
        "naive_copy_stake_usd": NAIVE_COPY_STAKE_USD,
        "per_slice_calib": PER_SLICE_CALIB,
        "vol_target": VOL_TARGET,
        "var_gate": VAR_GATE,
        "var_hard_cap_frac": VAR_HARD_CAP_FRAC,
        "ob_monitor": OB_MONITOR,
        "live_bankroll_usd": LIVE_BANKROLL_USD,
    }


if __name__ == "__main__":
    import json
    print(json.dumps(snapshot(), indent=2))
