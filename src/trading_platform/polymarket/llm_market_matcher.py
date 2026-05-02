"""LLM-based PM↔Kalshi semantic matcher.

Replaces the token-Jaccard heuristic in cross_platform_mapper for the
final disambiguation step. The Jaccard pass surfaces candidate pairs;
the LLM call confirms whether they are actually the same outcome
resolving on the same date.

Why an LLM here:
- "John Tory Toronto mayor 2026" PM ↔ "John Tory Toronto mayor 2027"
  Kalshi look identical to token-Jaccard but resolve a year apart.
- "Will Trump visit North Korea?" vs "Will Trump visit North Carolina?"
  share most tokens but mean different things.
- "Will the Fed cut rates by July 2026?" vs "Will the Fed cut by 25bps
  in July?" need rate-magnitude understanding.

LLM call shape:
  prompt: structured JSON-output asking for {same_outcome: bool,
          same_date: bool, confidence: 0-1, reason: str}
  cost: ~$0.001-0.003 per Haiku call

Run after cross_platform_mapper writes candidates with score 0.5-0.7
(the ambiguous middle band). High-confidence Jaccard matches (>=0.85)
get promoted directly; low (<0.5) get dropped; middle goes through LLM.

Requires ANTHROPIC_API_KEY in env. Skips silently if unset (returns
0 calls, no false positives).
"""
from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


ANTHROPIC_BASE = "https://api.anthropic.com/v1"
MODEL = "claude-haiku-4-5-20251001"
MAX_CALLS_PER_RUN = 50    # cost cap: ~$0.10/run worst case
SCORE_BAND_LOW = 0.50      # below this: drop
SCORE_BAND_HIGH = 0.85     # above this: auto-promote without LLM


_PROMPT = """Two prediction-market questions are listed below. PM is from \
Polymarket, Kalshi from Kalshi. Determine if they resolve to the SAME \
outcome on the SAME effective date.

PM:     "{pm_q}"
Kalshi: "{kalshi_title}"

Reply ONLY with JSON: {{"same_outcome": true|false, "same_date": \
true|false, "confidence": 0.0-1.0, "reason": "<one sentence>"}}.
"""


def _have_api_key() -> bool:
    return bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())


def _call_haiku(pm_q: str, kalshi_title: str) -> dict[str, Any] | None:
    if not _have_api_key():
        return None
    try:
        prompt = _PROMPT.format(pm_q=pm_q[:240], kalshi_title=kalshi_title[:240])
        body = json.dumps({
            "model": MODEL,
            "max_tokens": 200,
            "messages": [{"role": "user", "content": prompt}],
        }).encode("utf-8")
        req = urllib.request.Request(
            f"{ANTHROPIC_BASE}/messages",
            data=body,
            headers={
                "Content-Type": "application/json",
                "x-api-key": os.environ["ANTHROPIC_API_KEY"],
                "anthropic-version": "2023-06-01",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        text = (data.get("content") or [{}])[0].get("text", "").strip()
        # Strip code-fences / extract first { ... } block
        if "{" in text:
            text = text[text.index("{"):]
            if "}" in text:
                text = text[:text.rindex("}") + 1]
        return json.loads(text)
    except Exception as exc:
        logger.debug("[LLM_MATCHER] call failed: %s", exc)
        return None


def run_matcher() -> dict[str, Any]:
    if not _have_api_key():
        return {
            "skipped": "ANTHROPIC_API_KEY not set",
            "calls": 0, "promoted": 0,
        }
    t0 = time.time()
    conn = get_connection()
    n_calls = 0
    n_promoted = 0
    n_rejected = 0
    n_dropped = 0
    try:
        # Pull candidates in the ambiguous middle band that haven't been
        # llm-confirmed yet
        try:
            rows = conn.execute(
                """SELECT pm_condition_id, kalshi_ticker, score,
                          pm_question, kalshi_title
                     FROM cross_platform_market_map_candidates
                    WHERE score >= ? AND score < ?
                      AND pm_condition_id NOT IN (
                          SELECT pm_condition_id FROM cross_platform_market_map
                      )
                    ORDER BY score DESC LIMIT ?""",
                (SCORE_BAND_LOW, SCORE_BAND_HIGH, MAX_CALLS_PER_RUN),
            ).fetchall()
        except Exception as exc:
            return {"error": f"candidates query: {exc}"}

        now = int(time.time())
        for r in rows:
            pm_cid, k_ticker, score, pm_q, k_title = r
            verdict = _call_haiku(pm_q or "", k_title or "")
            n_calls += 1
            if verdict is None:
                continue
            same_outcome = bool(verdict.get("same_outcome"))
            same_date = bool(verdict.get("same_date"))
            llm_conf = float(verdict.get("confidence") or 0)
            reason = (verdict.get("reason") or "")[:200]
            if same_outcome and same_date and llm_conf >= 0.7:
                # Auto-promote to live arb pool
                try:
                    conn.execute(
                        """INSERT INTO cross_platform_market_map
                             (pm_condition_id, kalshi_ticker, quality, last_verified)
                           VALUES (?,?,?,?)
                           ON CONFLICT (pm_condition_id, kalshi_ticker)
                             DO UPDATE SET
                               quality = EXCLUDED.quality,
                               last_verified = EXCLUDED.last_verified""",
                        (pm_cid, k_ticker, round(llm_conf, 3), now),
                    )
                    n_promoted += 1
                    logger.info("[LLM_MATCHER] promoted pm=%s kalshi=%s conf=%.2f reason=%s",
                                pm_cid[:18], k_ticker[:24], llm_conf, reason[:80])
                except Exception as exc:
                    logger.debug("promote failed: %s", exc)
            elif not same_outcome or not same_date:
                n_rejected += 1
                # Update candidate with llm verdict so we don't re-call
                try:
                    conn.execute(
                        """UPDATE cross_platform_market_map_candidates
                             SET score = -1.0,
                                 last_evaluated = ?
                           WHERE pm_condition_id = ? AND kalshi_ticker = ?""",
                        (now, pm_cid, k_ticker),
                    )
                except Exception:
                    pass
            else:
                n_dropped += 1  # llm uncertain
        conn.commit()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "candidates_evaluated": len(rows),
            "llm_calls": n_calls,
            "promoted": n_promoted,
            "rejected": n_rejected,
            "uncertain": n_dropped,
            "estimated_cost_usd": round(n_calls * 0.0015, 3),
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_matcher())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
