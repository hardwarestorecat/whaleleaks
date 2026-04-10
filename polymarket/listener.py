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
from db import database as db
from store import flow_store
from utils.market_filter import is_geopolitical, is_sports, is_crypto, is_finance, is_tech, is_culture, matched_keywords

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
    price_frac   = price_cents / 100
    title        = trade.get("title", "")
    slug         = trade.get("slug", "")
    condition_id = (trade.get("conditionId") or "").lower()
    outcome      = (trade.get("outcome") or "").lower()
    side         = "yes" if outcome in ("yes",) else "no" if outcome in ("no",) else outcome.lower()
    trade_side   = (trade.get("side") or "BUY").upper()   # "BUY" | "SELL"
    maker        = (trade.get("proxyWallet") or "").lower()
    tx_hash      = trade.get("transactionHash", "")
    ts_raw       = trade.get("timestamp", 0)
    ts_dt        = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc)
    ts           = ts_dt.isoformat()
    quantity     = size
    # Look up cached market for API-sourced categories; fall back to keywords
    from polymarket.market_cache import lookup_market
    cached = lookup_market(condition_id)
    geo      = (cached["is_geopolitical"] if cached else False) or is_geopolitical(title)
    sports   = (cached["is_sports"]       if cached else False) or is_sports(title)
    politics = cached.get("is_politics", False) if cached else False
    crypto   = (cached.get("is_crypto", False)   if cached else False) or is_crypto(title)
    finance  = (cached.get("is_finance", False)   if cached else False) or is_finance(title)
    tech     = (cached.get("is_tech", False)      if cached else False) or is_tech(title)
    culture  = (cached.get("is_culture", False)   if cached else False) or is_culture(title)

    # ── Dedup: skip entirely if we've seen this tx before ────────────────────
    if usd_value >= config.FLOW_THRESHOLD_USD:
        fingerprint = f"{maker}:{usd_value:.2f}:{ts_raw}"
        key = tx_hash if tx_hash else fingerprint
        if await flow_store.is_whale_seen(key) or (tx_hash and await flow_store.is_whale_seen(fingerprint)):
            return
        await flow_store.mark_whale_seen(fingerprint)
        if tx_hash:
            await flow_store.mark_whale_seen(tx_hash)

    # ── Position tracking & fill persistence ─────────────────────────────────
    # Only write to SQLite for: whale-qualifying trades OR graduated whales.
    # Everything else stays in Redis only (flow feed, whale history).
    realized_pnl: float | None = None
    realized_cost: float | None = None

    if maker:
        is_grad = await flow_store.is_graduated_whale(maker)
        whale_spend = usd_value >= config.WHALE_THRESHOLD_USD
        save_to_db  = is_grad or whale_spend  # graduated whales always tracked; others only if big enough

        if trade_side == "SELL":
            # Always try to close position if we have one (need cost basis)
            if save_to_db or db.has_position(maker, condition_id, side):
                result = db.realize_sell(maker, condition_id, side, quantity, price_frac)
                if result is not None:
                    realized_pnl, realized_cost = result
                    outcome_val = "win" if realized_pnl > 0 else "loss"
                    if save_to_db:
                        db.save_fill(
                            tx_hash=tx_hash, block_number=0, address=maker, role="maker",
                            condition_id=condition_id, market_title=title, side=side,
                            trade_side="SELL", price_cents=price_cents, quantity=quantity,
                            usd_value=usd_value, ts=ts,
                            outcome=outcome_val, pnl_usd=round(realized_pnl, 2),
                        )
                        db.credit_realized_pnl(maker, realized_pnl, ts)
                    # Record P&L to graduation system regardless
                    if realized_pnl > 0:
                        await flow_store.record_pnl(maker, realized_pnl)
        else:
            # BUY
            if save_to_db:
                db.save_fill(
                    tx_hash=tx_hash, block_number=0, address=maker, role="maker",
                    condition_id=condition_id, market_title=title, side=side,
                    trade_side="BUY", price_cents=price_cents, quantity=quantity,
                    usd_value=usd_value, ts=ts,
                )
            # Always track position so we can calculate exit P&L later
            if save_to_db or usd_value >= config.FLOW_THRESHOLD_USD:
                db.upsert_position(maker, condition_id, side, quantity, usd_value)

    # ── Track largest fill per market (weekly high, Redis-backed) ────────────
    if usd_value >= config.FLOW_THRESHOLD_USD and trade_side == "BUY":
        pot_payout_h = round(usd_value / price_frac, 2) if price_frac else 0
        await flow_store.set_market_high(condition_id, {
            "usd_value": usd_value, "side": side,
            "price_cents": price_cents, "address": maker, "ts": ts,
            "potential_profit": round(pot_payout_h - usd_value, 2),
            "return_multiple": round(1 / price_frac, 2) if price_frac else 0,
        })

    # ── Flow panel — BUYs and profitable SELLs >= FLOW_THRESHOLD ─────────────
    if on_flow and usd_value >= config.FLOW_THRESHOLD_USD:
        if trade_side == "BUY":
            pot_payout = round(usd_value / price_frac, 2) if price_frac else 0
            flow_profit = round(pot_payout - usd_value, 2)
            flow_mult   = round(1 / price_frac, 2) if price_frac else 0
        else:
            # For sells, "potential profit" is the realized gain
            pot_payout  = usd_value
            flow_profit = round(realized_pnl, 2) if realized_pnl is not None else 0
            flow_mult   = round(realized_pnl / realized_cost, 2) if realized_cost else 0
        await on_flow({
            "tx_hash": tx_hash,
            "market_title": title,
            "market_id": slug or condition_id,
            "side": side,
            "trade_side": trade_side,
            "price_cents": price_cents,
            "usd_value": usd_value,
            "potential_payout": pot_payout,
            "potential_profit": flow_profit,
            "return_multiple": flow_mult,
            "address": maker,
            "ts": ts,
            "is_whale": usd_value >= config.WHALE_THRESHOLD_USD,
            "is_geopolitical": geo,
            "is_sports": sports,
            "is_politics": politics,
            "is_crypto": crypto,
            "is_finance": finance,
            "is_tech": tech,
            "is_culture": culture,
        })

    # ── Whale alert: BUYs with big spend+profit, OR profitable early exits ────
    if trade_side == "SELL":
        # Fire a whale alert for a profitable exit meeting criteria
        if (realized_pnl is not None
                and realized_cost is not None
                and realized_cost >= config.WHALE_THRESHOLD_USD
                and realized_pnl >= config.WHALE_MIN_PROFIT_USD):
            # quantity = cost + profit so that potential_profit property = realized_pnl
            exit_alert = WhaleAlert(
                source="polymarket",
                market_title=title,
                market_id=slug or condition_id,
                side=side,
                price_cents=price_cents,
                quantity=realized_cost + realized_pnl,
                usd_value=realized_cost,
                matched_keywords=matched_keywords(title),
                maker_address=maker,
                tx_hash=tx_hash,
            )
            if maker:
                stats = db.get_address_stats(maker)
                exit_alert.whale_address = maker
                if stats:
                    exit_alert.whale_win_rate      = stats["win_rate"]
                    exit_alert.whale_resolved_bets = stats["resolved_fills"]
                    exit_alert.whale_total_pnl     = stats["total_pnl_usd"]
            if not sports and on_whale:
                await on_whale(exit_alert)
        return

    # BUY path — must exceed spend threshold AND minimum profit
    if usd_value < config.WHALE_THRESHOLD_USD:
        return
    potential_profit_pre = quantity - usd_value
    if potential_profit_pre < config.WHALE_MIN_PROFIT_USD:
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

    if not sports:
        await dispatch(alert)

    if on_whale:
        await on_whale(alert)
