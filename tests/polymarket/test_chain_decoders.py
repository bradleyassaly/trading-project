"""Pin the on-chain log decoders against REAL captured logs.

The byte-level decode of OrderFilled / PositionSplit / PositionsMerge /
PayoutRedemption used to live inline in async handlers with no tests. On
2026-07-21 that hid a layout bug that silently dropped 100% of redemptions
(PayoutRedemption INDEXES the collateral token and carries the conditionId in
DATA — the OPPOSITE of split/merge). These tests pin every field of every event
type against logs captured off Polygon (see fixtures/README.md), and assert that
decoding an event with the WRONG layout fails loudly.

Pure-function tests: no DB, no network, no WalletStream instance — safe anywhere.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_platform.polymarket.wallet_stream import (
    CONDITION_RESOLUTION_TOPIC,
    CONDITIONAL_TOKENS,
    CTF_MERGE_TOPIC,
    CTF_REDEEM_TOPIC,
    CTF_SPLIT_TOPIC,
    ORDER_FILLED_TOPIC_V2,
    _CTF_COLLATERAL,
    decode_condition_resolution,
    decode_ctf_event,
    decode_order_filled,
    payout_yes_from_numerators,
)

FIX_DIR = Path(__file__).parent / "fixtures"

# Polymarket collateral tokens (lowercase) — the set the CTF filter keys on.
USDC_E = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"      # bridged USDC.e
USDC_NATIVE = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"  # native USDC
COLLATERAL_V2 = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb"
ZERO32 = "0x" + "0" * 64


def load(name: str) -> dict:
    """Return the ``log`` dict (address/topics/data/…) of a fixture."""
    with open(FIX_DIR / f"{name}.json", encoding="utf-8") as fh:
        return json.load(fh)["log"]


def load_full(name: str) -> dict:
    with open(FIX_DIR / f"{name}.json", encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Fixtures wire up to the real topic constants (the router keys on topic0)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name,topic", [
    ("order_filled_v2_buy_maker", ORDER_FILLED_TOPIC_V2),
    ("order_filled_v2_sell_maker", ORDER_FILLED_TOPIC_V2),
    ("order_filled_v2_token_to_token", ORDER_FILLED_TOPIC_V2),
    ("position_split", CTF_SPLIT_TOPIC),
    ("positions_merge", CTF_MERGE_TOPIC),
    ("payout_redemption", CTF_REDEEM_TOPIC),
    ("condition_resolution", CONDITION_RESOLUTION_TOPIC),
])
def test_fixture_topic0_matches_module_constant(name, topic):
    log = load(name)
    assert log["topics"][0].lower() == topic.lower()
    assert load_full(name)["topic0"].lower() == topic.lower()


# ---------------------------------------------------------------------------
# OrderFilled — maker BUY (real)
# ---------------------------------------------------------------------------

def test_order_filled_buy_maker_every_field():
    d = decode_order_filled(*(load("order_filled_v2_buy_maker")[k]
                              for k in ("topics", "data")))
    assert d is not None
    assert d["maker"] == "0x18e3861378b15f5a4ab5812ce006c7e8adfe1463"
    assert d["taker"] == "0xce25e214d5cfe4f459cf67f08df581885aae7fdc"
    assert d["maker_side"] == "BUY"
    assert d["maker_asset_id"] == 0                    # USDC leg on the maker
    assert d["taker_asset_id"] == d["token_id"]
    assert d["token_id"] == (
        0xef718cbaf116e210a6260a4becc1057833cf3d90661da2640abed2b15785cba3)
    assert d["usdc"] == pytest.approx(1.3356)
    assert d["shares"] == pytest.approx(3.18)
    assert d["price"] == pytest.approx(0.42)
    # price is internally consistent
    assert d["price"] == pytest.approx(d["usdc"] / d["shares"])


# ---------------------------------------------------------------------------
# OrderFilled — maker SELL (derived leg-swap; guards the SELL branch)
# ---------------------------------------------------------------------------

def test_order_filled_sell_maker_exercises_sell_branch():
    d = decode_order_filled(*(load("order_filled_v2_sell_maker")[k]
                              for k in ("topics", "data")))
    assert d is not None
    assert d["maker_side"] == "SELL"
    assert d["taker_asset_id"] == 0                    # USDC leg on the taker
    assert d["maker_asset_id"] == d["token_id"]
    # leg-swap of the buy-maker log ⇒ identical economics, opposite side
    assert d["token_id"] == (
        0xef718cbaf116e210a6260a4becc1057833cf3d90661da2640abed2b15785cba3)
    assert d["usdc"] == pytest.approx(1.3356)
    assert d["shares"] == pytest.approx(3.18)
    assert d["price"] == pytest.approx(0.42)


def test_sell_maker_fixture_is_labelled_derived():
    # Guard against anyone quietly presenting this synthetic log as a raw
    # capture — the V2 exchange never emits maker-SELL natively.
    meta = load_full("order_filled_v2_sell_maker")
    assert "DERIVED" in meta["source"]
    assert "_derivation" in meta


def test_buy_and_sell_fixtures_are_leg_swaps():
    buy = load("order_filled_v2_buy_maker")
    sell = load("order_filled_v2_sell_maker")
    db = decode_order_filled(buy["topics"], buy["data"])
    ds = decode_order_filled(sell["topics"], sell["data"])
    # Same trade economics, opposite decoded direction.
    assert db["maker_side"] == "BUY" and ds["maker_side"] == "SELL"
    for k in ("token_id", "usdc", "shares", "price"):
        assert db[k] == pytest.approx(ds[k]) if isinstance(db[k], float) else db[k] == ds[k]


# ---------------------------------------------------------------------------
# OrderFilled — token<->token (real): no USDC leg ⇒ None
# ---------------------------------------------------------------------------

def test_order_filled_token_to_token_is_skipped():
    log = load("order_filled_v2_token_to_token")
    d = decode_order_filled(log["topics"], log["data"])
    assert d is None


# ---------------------------------------------------------------------------
# Production SELL path with a REAL log: taker-role inversion (no fixture edit)
# ---------------------------------------------------------------------------

def test_real_buy_maker_is_a_sell_for_the_taker():
    """The V2 exchange never emits maker-SELL; a real SELL is the taker side of
    a maker-BUY log. This mirrors the handler's role-based inversion using the
    REAL buy-maker capture, so the SELL semantics are pinned with real bytes."""
    d = decode_order_filled(*(load("order_filled_v2_buy_maker")[k]
                              for k in ("topics", "data")))
    maker_side = d["maker_side"]                       # "BUY"
    # handler: side = maker_side if role == maker else inverted
    taker_side = "SELL" if maker_side == "BUY" else "BUY"
    assert taker_side == "SELL"


# ---------------------------------------------------------------------------
# CTF split / merge (real) — collateral in DATA word0, cid in topics[3]
# ---------------------------------------------------------------------------

def test_position_split_every_field():
    log = load("position_split")
    d = decode_ctf_event("split", log["topics"], log["data"])
    assert d == {
        "wallet": "0xada100874d00e3331d00f2007a9c336a65009718",
        "event_type": "split",
        "condition_id":
            "0x20f897a4dc277f6f220ff4a78fdb21c27f7654abbdb07f69bf4f14784b922858",
        "collateral": USDC_E,
        "amount": 190.0,
    }
    assert d["collateral"] in _CTF_COLLATERAL


def test_positions_merge_every_field():
    log = load("positions_merge")
    d = decode_ctf_event("merge", log["topics"], log["data"])
    assert d == {
        "wallet": "0xada100874d00e3331d00f2007a9c336a65009718",
        "event_type": "merge",
        "condition_id":
            "0xbfefe500c3fea4d8067f39246e4d9404a6fe0ea3040e1366d4ac02367d751c46",
        "collateral": USDC_E,
        "amount": 91.0,
    }
    assert d["collateral"] in _CTF_COLLATERAL


# ---------------------------------------------------------------------------
# CTF redeem (real) — collateral INDEXED (topics[2]), cid in DATA word0
# ---------------------------------------------------------------------------

def test_payout_redemption_every_field():
    log = load("payout_redemption")
    d = decode_ctf_event("redeem", log["topics"], log["data"])
    assert d == {
        "wallet": "0xada100db00ca00073811820692005400218fce1f",
        "event_type": "redeem",
        "condition_id":
            "0x44de1dbc622adb8cc35ace91a9327b57315a33af597e7f6d8663e78a2f1f1000",
        "collateral": USDC_E,      # from topics[2], NOT data — the layout that bit us
        "amount": pytest.approx(1.835442),
    }
    assert d["collateral"] in _CTF_COLLATERAL


# ---------------------------------------------------------------------------
# ConditionResolution (real) — the oracle report feeding market_resolutions
# ---------------------------------------------------------------------------

def test_condition_resolution_every_field():
    log = load("condition_resolution")
    d = decode_condition_resolution(log["topics"], log["data"])
    assert d == {
        "condition_id":
            "0xda44c9f8e255be5d94c716b0ee5f4849a84aa9f39572cb0def2c27161e2200c0",
        "oracle": "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7",
        "question_id":
            "0xad3689cae7a790eaada0eba6082a46deb6b014ce9b4a2dd9755bc12db33ef6bf",
        "outcome_slot_count": 2,
        "payout_numerators": [0, 1],
    }
    # [0, 1] = outcome slot 1 takes the pot ⇒ YES (slot 0) paid nothing.
    assert payout_yes_from_numerators(d["payout_numerators"]) == 0.0


def test_condition_resolution_fixture_from_canonical_contract():
    # The handler trusts ONLY the canonical ConditionalTokens deployment:
    # conditionId = keccak(oracle, questionId, slotCount) is portable across
    # CTF clones, so a copycat could emit inverted payouts for real cids.
    log = load("condition_resolution")
    assert log["address"].lower() == CONDITIONAL_TOKENS
    assert CONDITIONAL_TOKENS == CONDITIONAL_TOKENS.lower()


def test_payout_yes_from_numerators_semantics():
    assert payout_yes_from_numerators([1, 0]) == 1.0    # YES (slot 0) wins
    assert payout_yes_from_numerators([0, 1]) == 0.0    # NO wins
    assert payout_yes_from_numerators([1, 1]) == 0.5    # 50/50 tie/void
    assert payout_yes_from_numerators([1000000, 0]) == 1.0  # scale-proof
    assert payout_yes_from_numerators([3, 1]) == 0.75   # scalar-ish split
    assert payout_yes_from_numerators([0, 1], yes_index=1) == 1.0
    assert payout_yes_from_numerators([0, 0]) is None   # unresolvable garbage
    assert payout_yes_from_numerators([1, 0, 0]) is None  # non-binary
    assert payout_yes_from_numerators([]) is None


def test_condition_resolution_malformed_inputs_return_none():
    log = load("condition_resolution")
    assert decode_condition_resolution(log["topics"][:3], log["data"]) is None
    assert decode_condition_resolution(log["topics"], None) is None
    assert decode_condition_resolution(log["topics"], "0x") is None
    assert decode_condition_resolution(log["topics"], "0x" + "0" * 64) is None
    # truncated: length word says 2 numerators but only one word follows
    assert decode_condition_resolution(
        log["topics"], log["data"][:-64]) is None
    # array length disagreeing with outcomeSlotCount = mis-parse, fail loudly
    tampered = log["data"][:2 + 64 * 2] + ("0" * 63 + "3") + log["data"][2 + 64 * 3:]
    assert decode_condition_resolution(log["topics"], tampered) is None
    # misaligned / out-of-range offset word
    bad_offset = log["data"][:2 + 64] + ("0" * 62 + "41") + log["data"][2 + 64 * 2:]
    assert decode_condition_resolution(log["topics"], bad_offset) is None


def test_condition_resolution_honors_nonstandard_offset():
    """The decoder follows the ABI offset word instead of assuming 0x40 —
    same numerators relocated one word deeper must decode identically."""
    log = load("condition_resolution")
    d0 = decode_condition_resolution(log["topics"], log["data"])
    dh = log["data"].lower().replace("0x", "")
    words = [dh[i * 64:(i + 1) * 64] for i in range(len(dh) // 64)]
    # move the array head from word 2 to word 3 (offset 0x40 → 0x60) with a
    # junk padding word in between
    shifted = [words[0], "%064x" % 0x60, "f" * 64] + words[2:]
    d1 = decode_condition_resolution(log["topics"], "0x" + "".join(shifted))
    assert d1 == d0


def test_condition_resolution_normalizes_0x_and_case():
    log = load("condition_resolution")
    with_pref = decode_condition_resolution(log["topics"], log["data"])
    without = decode_condition_resolution(log["topics"], log["data"].replace("0x", ""))
    upper = decode_condition_resolution(log["topics"], log["data"].upper())
    assert with_pref == without == upper


# ---------------------------------------------------------------------------
# THE REGRESSION: decoding with the wrong layout must fail loudly
# ---------------------------------------------------------------------------

def test_redeem_decoded_with_split_layout_is_garbage():
    """2026-07-21 bug: parsing a PayoutRedemption with the split/merge layout.
    cid becomes the parentCollectionId (all-zero) and collateral becomes a
    conditionId fragment — so the Polymarket-collateral filter rejects it and
    100% of redeems vanish. This test fails if that layout ever comes back."""
    log = load("payout_redemption")
    correct = decode_ctf_event("redeem", log["topics"], log["data"])
    wrong = decode_ctf_event("split", log["topics"], log["data"])  # WRONG etype

    # The correct decode is a real Polymarket redemption…
    assert correct["collateral"] in _CTF_COLLATERAL
    assert correct["condition_id"] != ZERO32

    # …the wrong-layout decode is garbage on BOTH keys.
    assert wrong["condition_id"] == ZERO32                 # read parentCollectionId
    assert wrong["condition_id"] != correct["condition_id"]
    assert wrong["collateral"] not in _CTF_COLLATERAL      # a cid fragment, filtered out
    assert wrong["collateral"] != correct["collateral"]


def test_split_decoded_with_redeem_layout_is_garbage():
    """Symmetric guard: a PositionSplit parsed with the redeem layout also
    mangles both fields (collateral←parentCollectionId zeros, cid←data word0)."""
    log = load("position_split")
    correct = decode_ctf_event("split", log["topics"], log["data"])
    wrong = decode_ctf_event("redeem", log["topics"], log["data"])  # WRONG etype

    assert correct["collateral"] in _CTF_COLLATERAL
    assert wrong["collateral"] not in _CTF_COLLATERAL      # parentCollectionId (zeros)
    assert wrong["condition_id"] != correct["condition_id"]


# ---------------------------------------------------------------------------
# _CTF_COLLATERAL membership set (the intrinsic, delisting-proof filter)
# ---------------------------------------------------------------------------

def test_ctf_collateral_set_contents():
    assert _CTF_COLLATERAL == {USDC_E, USDC_NATIVE, COLLATERAL_V2}
    assert all(a == a.lower() for a in _CTF_COLLATERAL)


def test_all_ctf_fixtures_carry_polymarket_collateral():
    for name, et in (("position_split", "split"),
                     ("positions_merge", "merge"),
                     ("payout_redemption", "redeem")):
        log = load(name)
        d = decode_ctf_event(et, log["topics"], log["data"])
        assert d["collateral"] in _CTF_COLLATERAL, name


# ---------------------------------------------------------------------------
# Malformed / defensive inputs → None (no exceptions leak to the handler)
# ---------------------------------------------------------------------------

def test_order_filled_too_few_topics_returns_none():
    log = load("order_filled_v2_buy_maker")
    assert decode_order_filled(log["topics"][:3], log["data"]) is None


def test_order_filled_short_data_returns_none():
    log = load("order_filled_v2_buy_maker")
    assert decode_order_filled(log["topics"], "0x" + "0" * (64 * 4)) is None
    assert decode_order_filled(log["topics"], None) is None
    assert decode_order_filled(log["topics"], "0x") is None


def test_ctf_too_few_topics_returns_none():
    log = load("payout_redemption")
    assert decode_ctf_event("redeem", log["topics"][:3], log["data"]) is None


def test_ctf_short_data_returns_none():
    log = load("position_split")
    assert decode_ctf_event("split", log["topics"], "0x" + "0" * (64 * 2)) is None
    assert decode_ctf_event("split", log["topics"], None) is None


def test_decoders_normalize_0x_and_case():
    """data may arrive with/without 0x and in mixed case; decode is stable."""
    log = load("order_filled_v2_buy_maker")
    with_pref = decode_order_filled(log["topics"], log["data"])
    without = decode_order_filled(log["topics"], log["data"].replace("0x", ""))
    upper = decode_order_filled(log["topics"], log["data"].upper())
    assert with_pref == without == upper
