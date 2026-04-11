"""
Background task that polls Polymarket's Gamma API to detect resolved markets
and credit wins/losses to tracked whale addresses.

Runs every POLL_INTERVAL seconds, checks all condition IDs that still have
unresolved fills in the DB.

Resolution logic
────────────────
Gamma API market object (closed market):
  {
    "conditionId": "0x...",
    "active": false,
    "closed": true,
    "tokens": [
      {"token_id": "1234...", "outcome": "Yes", "price": 1.0},
      {"token_id": "5678...", "outcome": "No",  "price": 0.0}
    ]
  }
The token whose price == 1.0 (or nearest) is the winner.
If both are 0.5 the market is invalid/pushed.
"""
from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timezone

import aiohttp

import config
from db import database as db

log = logging.getLogger("polymarket.resolver")

POLL_INTERVAL = 600   # check every 10 minutes


async def run() -> None:
    """Run forever; poll for resolved markets on interval."""
    log.warning("Resolver started — polling every %ds", POLL_INTERVAL)
    while True:
        try:
            await _resolve_pending()
        except Exception as exc:
            log.error("Resolver error: %s", exc, exc_info=True)
        await asyncio.sleep(POLL_INTERVAL)


async def _resolve_pending() -> None:
    condition_ids = db.get_unresolved_condition_ids()
    if not condition_ids:
        return

    log.info("Resolver checking %d unresolved conditions", len(condition_ids))
    now = datetime.now(timezone.utc)

    async with aiohttp.ClientSession() as session:
        for cid in condition_ids:
            # Skip only if checked very recently (within this poll interval) —
            # a prior bug cached everything as None on first run and then skipped
            # it forever, so we must re-check across cycles.
            cached = db.get_cached_outcome(cid)
            if cached and cached["winning_side"] is None:
                try:
                    checked_at = datetime.fromisoformat(cached["checked_at"])
                    if checked_at.tzinfo is None:
                        checked_at = checked_at.replace(tzinfo=timezone.utc)
                    age_s = (now - checked_at).total_seconds()
                    if age_s < POLL_INTERVAL:
                        continue  # checked within this cycle, truly skip
                except Exception:
                    pass  # malformed timestamp — fall through and re-check

            await _check_one(session, cid, now.isoformat())
            await asyncio.sleep(0.2)   # rate-limit Gamma API


async def _check_one(session: aiohttp.ClientSession, condition_id: str, now: str) -> None:
    url = f"{config.POLYMARKET_GAMMA_API}/markets?conditionId={condition_id}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return
            markets = await r.json()
    except Exception as exc:
        log.warning("Gamma API error for %s: %s", condition_id, exc)
        return

    if not markets:
        return

    market = markets[0] if isinstance(markets, list) else markets

    active = market.get("active", True)
    closed = market.get("closed", False)

    if active or not closed:
        # Still open — cache as unresolved, skip
        db.cache_outcome(condition_id, None, now)
        return

    winning_side = _determine_winner(market)
    db.cache_outcome(condition_id, winning_side, now)

    if winning_side:
        wins = db.apply_outcome(condition_id, winning_side, now)
        log.info("Resolved %s → %s (%d wins)", condition_id[:16], winning_side, len(wins))
        # Feed wins into the Redis graduation system
        if wins:
            from store import flow_store
            import asyncio
            for w in wins:
                try:
                    asyncio.get_event_loop().create_task(
                        flow_store.record_pnl(w["address"], w["pnl"])
                    )
                except Exception:
                    pass


def _determine_winner(market: dict) -> str | None:
    tokens = market.get("tokens") or market.get("outcomePrices") or []

    # tokens is a list of {"outcome": "Yes"/"No", "price": float}
    if isinstance(tokens, list) and tokens and isinstance(tokens[0], dict):
        winner = None
        best_price = -1.0
        for tok in tokens:
            price = float(tok.get("price", 0))
            if price > best_price:
                best_price = price
                winner = tok.get("outcome", "").lower()
        if best_price < 0.9:
            return "invalid"   # neither outcome clearly won
        return winner   # "yes" or "no"

    # Fallback: outcomePrices might be ["1", "0"] or ["0", "1"]
    if isinstance(tokens, list) and len(tokens) >= 2:
        try:
            prices = [float(p) for p in tokens]
            if prices[0] > prices[1]:
                return "yes"
            elif prices[1] > prices[0]:
                return "no"
        except (ValueError, TypeError):
            pass

    return None
