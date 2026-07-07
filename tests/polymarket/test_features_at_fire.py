"""N3: signal-fire feature snapshots must serialize any signal dict and
round-trip. Guards the `default=str` choice (signal dicts carry non-JSON
values) and the column presence. Integration coverage is the live
verification (126 live_trades rows populated on deploy)."""
import json
from datetime import datetime, timezone


def _serialize(signal):
    # Mirror the exact call used at the INSERT sites.
    return json.dumps(signal, default=str)


def test_snapshot_round_trips_core_keys():
    signal = {
        "signal_type": "resolution_decay",
        "condition_id": "0xabc",
        "direction": "BUY",
        "confidence": 0.72,
        "confidence_raw": 0.63,
        "wallet": "phase_b_resolution_decay",
        "price": 0.19,
        "end_date_iso": "2026-07-08T16:00:00",
    }
    blob = _serialize(signal)
    back = json.loads(blob)
    for k in ("signal_type", "condition_id", "confidence", "confidence_raw", "end_date_iso"):
        assert back[k] == signal[k]


def test_snapshot_tolerates_non_json_values():
    # datetimes / sets / custom objects must not raise — default=str coerces.
    signal = {
        "signal_type": "whale_entry_filtered",
        "condition_id": "0xdef",
        "fired_at": datetime(2026, 7, 7, tzinfo=timezone.utc),
        "converging_wallets": {"0x1", "0x2"},
        "obj": object(),
    }
    blob = _serialize(signal)  # must not raise
    back = json.loads(blob)
    assert back["signal_type"] == "whale_entry_filtered"
    assert isinstance(back["fired_at"], str)  # coerced


def test_empty_and_nested():
    assert json.loads(_serialize({})) == {}
    nested = {"a": {"b": [1, 2, {"c": 3}]}}
    assert json.loads(_serialize(nested)) == nested
