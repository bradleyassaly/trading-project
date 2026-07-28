"""Chain-truth resolutions from PayoutRedemption cash flow (D2, 2026-07-27).

STATUS 2026-07-28: NEGATIVE RESULT — kept as an investigation tool, NOT
scheduled, --apply NOT trusted. Validation abstained on 100% of
candidates (500 all-history, 300 fresh, 32 own-conditions) and the
diagnosis is structural, in escalating order of discovery:
  1. retention prunes non-roster trades >7d, so long-accumulation
     positions can't be matched by redemption time (fixable by scoping);
  2. many redeemers acquired via split/transfer, not CLOB fills
     (correct abstentions); and — terminally —
  3. the DOMINANT redemption path is Polymarket's settlement relayer
     (0xa1200000d0…, single redeems up to $520K on our own conditions)
     redeeming ON BEHALF of users: the on-chain redeemer has NO trading
     position, so cash↔position attribution is impossible for most
     volume.
The correct chain-truth source is the CTF ConditionResolution event
(payoutNumerators straight from the oracle report, zero inference) —
a wallet_stream decoder addition; see the 2026-07-28 chip. This module's
worth is the documented dead end + the relayer discovery.

Gamma DELISTS markets once they resolve — the 2026-07-21 eval found ~50%
of concluded traded markets missing from Gamma-based resolution sources,
masked twice over. clob_winner (rank 80) patched most of it, but gaps
remain (exit_counterfactual reported 6 unresolved closed trades on
2026-07-27). This module derives the winner from data no one can delist:
the on-chain redemption cash flow our firehose already records.

Inference: a PayoutRedemption pays $1 per WINNING share. wallet_ctf_events
stores (wallet, condition, amount_usdc) per redeem — no index sets — so
the winning SIDE is attributed by matching each redeemer's cash against
their net token position from wallet_trades:

    net_yes ≈ amount_usdc and net_no ≈ 0  ⇒ that redeemer held YES ⇒ YES won
    (symmetric for NO; mixed/mismatched positions abstain)

Verdict per condition = agreement among the top redeemers (weighted by
cash, dissent ⇒ skip + log). Writes via resolutions.record_resolution
(source='chain_redeem', rank 65): write-once-wins means it only ever
FILLS GAPS below the direct flag reads and never overrides them; a
disagreement with a higher-rank row lands in market_resolution_conflicts.

Modes:
  --validate       score the inference against conditions that ALREADY
                   have a rank>=70 resolution (no writes) — the golden
                   test that gates trusting --apply
  --apply          fill conditions lacking a rank>=65 resolution
  --limit N        max conditions per pass (default 500)

Runs in the slow lane (daily, after --validate acceptance).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket.resolutions import record_resolution

logger = logging.getLogger(__name__)

# A redeemer votes only when their winning-side net position explains the
# cash within TOL and the other side is ≤ CROSS_MAX of it (near-pure).
MATCH_TOL = 0.15
CROSS_MAX = 0.10
MIN_REDEEM_USD = 1.0        # dust redeems carry no signal
TOP_REDEEMERS = 5
# Verdict needs either two agreeing voters, or one voter with a tight
# match and real size (our own auto-redeems are usually this case).
SINGLE_VOTER_MIN_USD = 5.0
SINGLE_VOTER_TOL = 0.05


def _voter_side(conn, wallet: str, cid: str, yes_tok: str, no_tok: str,
                usdc: float, tol: float = MATCH_TOL) -> str | None:
    rows = conn.execute(
        """SELECT asset, SUM(CASE WHEN side = 'BUY' THEN size ELSE -size END)
             FROM wallet_trades
            WHERE wallet = ? AND condition_id = ?
            GROUP BY asset""",
        (wallet, cid),
    ).fetchall()
    net_yes = net_no = 0.0
    for asset, net in rows:
        if str(asset) == str(yes_tok):
            net_yes = float(net or 0)
        elif str(asset) == str(no_tok):
            net_no = float(net or 0)

    def _match(win_net: float, other_net: float) -> bool:
        if win_net <= 0:
            return False
        if abs(win_net - usdc) / max(usdc, 1.0) > tol:
            return False
        return abs(other_net) <= CROSS_MAX * win_net

    if _match(net_yes, net_no):
        return "YES"
    if _match(net_no, net_yes):
        return "NO"
    return None


def _infer_condition(conn, cid: str, yes_tok: str, no_tok: str,
                     ) -> tuple[str | None, dict[str, Any]]:
    """(winner 'YES'|'NO'|None, evidence). Dissent or no confident voter
    ⇒ None."""
    redeemers = conn.execute(
        """SELECT wallet, SUM(amount_usdc) AS usdc, MAX(timestamp) AS ts
             FROM wallet_ctf_events
            WHERE condition_id = ? AND event_type = 'redeem'
              AND amount_usdc >= ?
            GROUP BY wallet
            ORDER BY usdc DESC
            LIMIT ?""",
        (cid, MIN_REDEEM_USD, TOP_REDEEMERS),
    ).fetchall()
    votes: list[tuple[str, float, str]] = []
    last_ts = 0
    for wallet, usdc, ts in redeemers:
        usdc = float(usdc or 0)
        last_ts = max(last_ts, int(ts or 0))
        side = _voter_side(conn, str(wallet), cid, yes_tok, no_tok, usdc)
        if side:
            votes.append((str(wallet), usdc, side))
    ev: dict[str, Any] = {
        "voters": [{"w": v[0][:10], "usd": round(v[1], 2), "side": v[2]}
                   for v in votes],
        "redeemers_seen": len(redeemers),
        "last_redeem_ts": last_ts,
    }
    if not votes:
        return None, ev
    sides = {v[2] for v in votes}
    if len(sides) > 1:
        ev["dissent"] = True
        return None, ev
    side = votes[0][2]
    if len(votes) >= 2:
        return side, ev
    # single voter: require size + a tight cash↔position match
    w, usdc, _ = votes[0]
    if usdc >= SINGLE_VOTER_MIN_USD and _voter_side(
            conn, w, cid, yes_tok, no_tok, usdc,
            tol=SINGLE_VOTER_TOL) == side:
        return side, ev
    ev["single_voter_below_bar"] = True
    return None, ev


def run(mode: str, limit: int = 500, days: int = 3,
        db_path: str | None = None) -> dict:
    """days: only conditions with a redeem in the last N days. This is not
    just a speed knob — non-roster wallet_trades older than 7d are pruned
    by firehose retention, so position-matching only has full evidence for
    FRESHLY-redeemed conditions. The daily slow-lane run catches each new
    resolution inside that window; old gaps would need the parquet
    archive (out of scope). First full-history validate run (2026-07-28):
    500/500 abstained because the un-filtered candidate scan surfaced
    long-pruned conditions + operational redeemers (split/transfer
    acquirers with zero CLOB fills — correct abstentions)."""
    t0 = time.time()
    now = int(time.time())
    fresh_cut = now - days * 86400
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        if mode == "validate":
            # Conditions with BOTH fresh redeems and an authoritative row.
            cands = conn.execute(
                """SELECT ce.condition_id, m.yes_token_id,
                          m.no_token_id, mr.resolves_yes
                     FROM wallet_ctf_events ce
                     JOIN market_resolutions mr
                       ON mr.condition_id = ce.condition_id
                      AND mr.source_rank >= 70
                      AND mr.resolves_yes IS NOT NULL
                     JOIN markets m ON m.condition_id = ce.condition_id
                    WHERE ce.event_type = 'redeem'
                      AND ce.timestamp >= ?
                      AND m.yes_token_id IS NOT NULL
                      AND m.no_token_id IS NOT NULL
                    GROUP BY ce.condition_id, m.yes_token_id,
                             m.no_token_id, mr.resolves_yes
                    ORDER BY MAX(ce.timestamp) DESC
                    LIMIT ?""",
                (fresh_cut, limit),
            ).fetchall()
            agree = disagree = abstain = 0
            disagreements = []
            for cid, yes_tok, no_tok, truth_yes in cands:
                winner, ev = _infer_condition(conn, cid, yes_tok, no_tok)
                if winner is None:
                    abstain += 1
                    continue
                truth = "YES" if int(truth_yes) else "NO"
                if winner == truth:
                    agree += 1
                else:
                    disagree += 1
                    disagreements.append({"cid": cid, "inferred": winner,
                                          "truth": truth, "ev": ev})
            out = {
                "mode": "validate", "candidates": len(cands),
                "agree": agree, "disagree": disagree, "abstain": abstain,
                "accuracy_on_votes": round(agree / (agree + disagree), 4)
                if (agree + disagree) else None,
                "disagreements": disagreements[:10],
                "elapsed_seconds": round(time.time() - t0, 1),
            }
            logger.info("[chain_res] %s", json.dumps(out)[:800])
            return out

        # apply: fill FRESHLY-redeemed conditions lacking a rank>=65 row.
        cands = conn.execute(
            """SELECT ce.condition_id, m.yes_token_id, m.no_token_id
                 FROM wallet_ctf_events ce
                 JOIN markets m ON m.condition_id = ce.condition_id
                 LEFT JOIN market_resolutions mr
                        ON mr.condition_id = ce.condition_id
                WHERE ce.event_type = 'redeem'
                  AND ce.timestamp >= ?
                  AND m.yes_token_id IS NOT NULL
                  AND m.no_token_id IS NOT NULL
                  AND (mr.condition_id IS NULL OR mr.source_rank < 65)
                GROUP BY ce.condition_id, m.yes_token_id, m.no_token_id
                ORDER BY MAX(ce.timestamp) DESC
                LIMIT ?""",
            (fresh_cut, limit),
        ).fetchall()
        written = skipped = 0
        for cid, yes_tok, no_tok in cands:
            winner, ev = _infer_condition(conn, cid, yes_tok, no_tok)
            if winner is None:
                skipped += 1
                continue
            resolves_yes = 1 if winner == "YES" else 0
            record_resolution(
                cid,
                source="chain_redeem",
                resolves_yes=resolves_yes,
                payout_yes=float(resolves_yes),
                winning_outcome="Yes" if resolves_yes else "No",
                yes_token_id=str(yes_tok),
                no_token_id=str(no_tok),
                resolved_at=ev.get("last_redeem_ts") or None,
                details=ev,
                db_path=db_path,
            )
            written += 1
        out = {"mode": "apply", "candidates": len(cands),
               "written": written, "abstained": skipped,
               "elapsed_seconds": round(time.time() - t0, 1)}
        logger.info("[chain_res] %s", json.dumps(out))
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--days", type=int, default=3)
    args = ap.parse_args()
    mode = "apply" if args.apply else "validate"
    print(json.dumps(run(mode, limit=args.limit, days=args.days), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
