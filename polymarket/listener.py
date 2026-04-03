"""
Polymarket whale listener — polls data-api.polymarket.com/trades

No auth required. Returns all recent trades across all markets with
title, price, size, wallet, and tx hash included.

Dollar value = size × price  (size is in shares, price is 0-1 USDC)
"""
from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Awaitable

import aiohttp

import config
from alerts.models import WhaleAlert
from alerts.dispatcher import dispatch
from dashboard import record_fill
from db import database as db
from utils.market_filter import is_geopolitical, matched_keywords

log = logging.getLogger("polymarket.listener")

WhaleCB = Callable[[WhaleAlert], Awaitable[None]]
FlowCB  = Callable[[dict], Awaitable[None]]

DATA_API = "https://data-api.polymarket.com"
POLL_INTERVAL = 3   # seconds


async def run(on_whale: WhaleCB | None = None, on_flow: FlowCB | None = None) -> None:
    while True:
        try:
            await _run_once(on_whale, on_flow)
        except Exception as exc:
            log.error("Listener error: %s — restarting in 10s", exc)
        await asyncio.sleep(10)


async def _run_once(on_whale: WhaleCB | None, on_flow: FlowCB | None) -> None:
    seen: set[str] = set()
    log.info("Polymarket data-api listener started (polling every %ds)", POLL_INTERVAL)

    async with aiohttp.ClientSession() as session:
        # Seed seen set from current latest trades so we don't replay history
        trades = await _fetch(session, limit=200)
        for t in trades:
            seen.add(t.get("transactionHash", ""))
        log.info("Seeded %d existing trades, watching for new ones", len(seen))

        while True:
            await asyncio.sleep(POLL_INTERVAL)
            trades = await _fetch(session, limit=200)
            new_trades = [t for t in trades if t.get("transactionHash", "") not in seen]

            for trade in new_trades:
                tx = trade.get("transactionHash", "")
                seen.add(tx)
                await _handle(trade, on_whale, on_flow)

            if new_trades:
                log.debug("%d new trades", len(new_trades))


async def _fetch(session: aiohttp.ClientSession, limit: int = 200) -> list[dict]:
    try:
        async with session.get(
            f"{DATA_API}/trades",
            params={"limit": limit},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                body = await r.text()
                log.warning("data-api %s — %s", r.status, body[:150])
                return []
            return await r.json()
    except Exception as exc:
        log.warning("data-api fetch error: %s", exc)
        return []


async def _handle(trade: dict, on_whale: WhaleCB | None, on_flow: FlowCB | None) -> None:
    try:
        size  = float(trade.get("size", 0))
        price = float(trade.get("price", 0))
    except (TypeError, ValueError):
        return

    usd_value = size * price
    if usd_value < 1:
        return

    price_cents  = int(price * 100)
    title        = trade.get("title", "")
    slug         = trade.get("slug", "")
    condition_id = (trade.get("conditionId") or "").lower()
    outcome      = (trade.get("outcome") or "").lower()
    side         = "yes" if outcome in ("yes",) else "no" if outcome in ("no",) else outcome.lower()
    maker        = (trade.get("proxyWallet") or "").lower()
    tx_hash      = trade.get("transactionHash", "")
    ts_raw       = trade.get("timestamp", 0)
    ts_dt        = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc)
    ts           = ts_dt.isoformat()
    quantity     = size
    geo          = is_geopolitical(title)

    # Persist for win-rate tracking
    if usd_value >= 100 and maker:
        db.save_fill(
            tx_hash=tx_hash, block_number=0, address=maker, role="maker",
            condition_id=condition_id, market_title=title, side=side,
            price_cents=price_cents, quantity=quantity,
            usd_value=usd_value, ts=ts,
        )

    # Track largest fill per market
    if usd_value >= config.FLOW_THRESHOLD_USD:
        record_fill(condition_id, {
            "usd_value": usd_value, "side": side,
            "price_cents": price_cents, "address": maker, "ts": ts,
        })

    # Flow panel — everything >= FLOW_THRESHOLD
    if on_flow and usd_value >= config.FLOW_THRESHOLD_USD:
        await on_flow({
            "market_title": title,
            "market_id": slug or condition_id,
            "side": side,
            "price_cents": price_cents,
            "usd_value": usd_value,
            "address": maker,
            "ts": ts,
            "is_whale": usd_value >= config.WHALE_THRESHOLD_USD,
            "is_geopolitical": geo,
        })

    # Whale alert — any trade >= threshold
    if usd_value < config.WHALE_THRESHOLD_USD:
        return

    alert = WhaleAlert(
        source="polymarket",
        market_title=title,
        market_id=slug or condition_id,
        side=side,
        price_cents=price_cents,
        quantity=quantity,
        usd_value=usd_value,
        matched_keywords=matched_keywords(title),
        maker_address=maker,
        tx_hash=tx_hash,
    )

    if maker:
        stats = db.get_address_stats(maker)
        alert.whale_address = maker
        if stats:
            alert.whale_win_rate    = stats["win_rate"]
            alert.whale_resolved_bets = stats["resolved_fills"]
            alert.whale_total_pnl   = stats["total_pnl_usd"]

    # Discord only for geopolitical markets
    if geo:
        await dispatch(alert)

    if on_whale:
        await on_whale(alert)
