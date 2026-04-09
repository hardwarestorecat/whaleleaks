"""
Redis-backed persistence for order flow and whale alerts.
Uses sorted sets (score = Unix timestamp) for time-based retention.
Gracefully degrades to no-op if Redis is unavailable.
"""
from __future__ import annotations
import json
import logging
import time

import redis.asyncio as aioredis

import config

import redis as redis_sync

log = logging.getLogger("store.flow")

FLOW_KEY         = "whaleleaks:flow"        # ZSET: member=fingerprint, score=ts
FLOW_DATA_KEY    = "whaleleaks:flow_data"   # HASH: fingerprint → json
WHALE_KEY        = "whaleleaks:whales"
WHALE_DATA_KEY   = "whaleleaks:whale_data"
MARKET_HIGH_KEY  = "whaleleaks:market_high"
GRAD_KEY         = "whaleleaks:graduates"   # ZSET: address → 30-day cumulative P&L
GRAD_PNL_KEY     = "whaleleaks:grad_pnl"   # ZSET: "{address}:{ts}" → pnl (rolling window)
TTL              = 14 * 24 * 3600   # 14 days in seconds (flow/whale retention window)
WEEK             = 7  * 24 * 3600   # 7 days in seconds
WHALE_GRAD_USD   = 10_000.0         # cumulative 30-day P&L to become a graduated whale


def _flow_key(d: dict) -> str:
    """Stable dedup key for a flow/whale entry."""
    return (d.get("tx_hash") or
            f"{d.get('address','')}:{d.get('usd_value',0):.2f}:{d.get('ts','')}")


def _url() -> str:
    return getattr(config, "REDIS_URL", "redis://localhost:6379")


# ── Async writes (called from main asyncio loop) ──────────────────────────────

async def push_flow(flow: dict) -> None:
    try:
        r = aioredis.from_url(_url(), decode_responses=True)
        ts = time.time()
        fk = _flow_key(flow)
        async with r:
            added = await r.zadd(FLOW_KEY, {fk: ts}, nx=True)
            if added:
                await r.hset(FLOW_DATA_KEY, fk, json.dumps(flow))
                await r.expire(FLOW_DATA_KEY, TTL)
            await r.zremrangebyscore(FLOW_KEY, 0, ts - TTL)
    except Exception as exc:
        log.debug("Redis flow push: %s", exc)


async def push_whale(alert: dict) -> None:
    try:
        r = aioredis.from_url(_url(), decode_responses=True)
        ts = time.time()
        fk = _flow_key(alert)
        async with r:
            added = await r.zadd(WHALE_KEY, {fk: ts}, nx=True)
            if added:
                await r.hset(WHALE_DATA_KEY, fk, json.dumps(alert))
                await r.expire(WHALE_DATA_KEY, TTL)
            await r.zremrangebyscore(WHALE_KEY, 0, ts - TTL)
    except Exception as exc:
        log.debug("Redis whale push: %s", exc)


SEEN_TX_KEY = "whaleleaks:seen_txs"


async def is_whale_seen(tx_hash: str) -> bool:
    """Return True if this tx_hash was already processed as a whale alert."""
    if not tx_hash:
        return False
    try:
        r = aioredis.from_url(_url(), decode_responses=True)
        async with r:
            return bool(await r.sismember(SEEN_TX_KEY, tx_hash))
    except Exception as exc:
        log.debug("Redis seen-tx check: %s", exc)
        return False


async def mark_whale_seen(tx_hash: str) -> None:
    """Record a tx_hash as processed; expires after 7 days."""
    if not tx_hash:
        return
    try:
        r = aioredis.from_url(_url(), decode_responses=True)
        async with r:
            await r.sadd(SEEN_TX_KEY, tx_hash)
            await r.expire(SEEN_TX_KEY, WEEK)
    except Exception as exc:
        log.debug("Redis mark-seen: %s", exc)


async def set_market_high(condition_id: str, fill: dict) -> None:
    """Store the highest-value fill per market for up to 7 days from the fill's timestamp."""
    try:
        r = aioredis.from_url(_url(), decode_responses=True)
        key = f"{MARKET_HIGH_KEY}:{condition_id}"
        async with r:
            existing = await r.get(key)
            if existing:
                current = json.loads(existing)
                if fill["usd_value"] <= current["usd_value"]:
                    return
            # TTL anchored to fill timestamp so old highs expire naturally
            try:
                from datetime import datetime as _dt
                fill_ts = _dt.fromisoformat(fill["ts"]).timestamp()
            except Exception:
                fill_ts = time.time()
            ttl = int(fill_ts + WEEK - time.time())
            if ttl <= 0:
                return  # fill is already older than a week
            await r.set(key, json.dumps(fill), ex=ttl)
    except Exception as exc:
        log.debug("Redis market high set: %s", exc)


# ── Sync reads (called from FastAPI/uvicorn thread) ───────────────────────────

def get_recent_flow(limit: int = 500) -> list[dict]:
    try:
        r = redis_sync.from_url(_url(), decode_responses=True, socket_connect_timeout=2)
        cutoff = time.time() - TTL
        keys = r.zrevrangebyscore(FLOW_KEY, "+inf", cutoff, start=0, num=limit)
        if not keys:
            return []
        values = r.hmget(FLOW_DATA_KEY, keys)
        return [json.loads(v) for v in values if v]
    except Exception as exc:
        log.debug("Redis flow fetch: %s", exc)
        return []


def get_all_market_highs() -> dict[str, dict]:
    """Return {condition_id: fill} for all markets with a recorded weekly high."""
    try:
        r = redis_sync.from_url(_url(), decode_responses=True, socket_connect_timeout=2)
        keys = r.keys(f"{MARKET_HIGH_KEY}:*")
        if not keys:
            return {}
        values = r.mget(keys)
        prefix_len = len(MARKET_HIGH_KEY) + 1
        return {
            key[prefix_len:]: json.loads(val)
            for key, val in zip(keys, values)
            if val
        }
    except Exception as exc:
        log.debug("Redis market highs fetch: %s", exc)
        return {}


async def record_pnl(address: str, pnl: float) -> bool:
    """
    Add a realized P&L event to the rolling 30-day window for an address.
    Returns True if the address crosses the graduation threshold ($10k).
    """
    if not address or pnl <= 0:
        return False
    try:
        r = aioredis.from_url(_url(), decode_responses=True)
        now = time.time()
        cutoff = now - TTL
        member = f"{address}:{now}"
        async with r:
            # Add this P&L event (score = pnl, sorted by timestamp in member)
            await r.zadd(GRAD_PNL_KEY, {member: pnl})
            # Trim entries older than 30 days by removing members whose timestamp is old
            # We store ts in the member string so use a Lua scan instead — simpler: store ts as score
            # Restructure: use a per-address sorted set with score=ts, value=pnl serialized
            addr_key = f"whaleleaks:addr_pnl:{address}"
            await r.zadd(addr_key, {str(pnl): now})
            await r.expire(addr_key, TTL)
            # Remove events older than 30 days
            await r.zremrangebyscore(addr_key, 0, cutoff)
            # Sum remaining P&L values (stored as member strings)
            entries = await r.zrange(addr_key, 0, -1)
            total_30d = sum(float(e) for e in entries)
            # Update the graduates leaderboard
            await r.zadd(GRAD_KEY, {address: total_30d})
            await r.expire(GRAD_KEY, TTL)
            return total_30d >= WHALE_GRAD_USD
    except Exception as exc:
        log.debug("Redis record_pnl: %s", exc)
        return False


async def is_graduated_whale(address: str) -> bool:
    """Return True if this address has >$10k cumulative P&L in the last 30 days."""
    if not address:
        return False
    try:
        r = aioredis.from_url(_url(), decode_responses=True)
        async with r:
            score = await r.zscore(GRAD_KEY, address)
            return score is not None and float(score) >= WHALE_GRAD_USD
    except Exception as exc:
        log.debug("Redis is_graduated_whale: %s", exc)
        return False


def get_graduated_whales() -> list[dict]:
    """Return all graduated whales sorted by 30-day P&L, for the leaderboard."""
    try:
        r = redis_sync.from_url(_url(), decode_responses=True, socket_connect_timeout=2)
        entries = r.zrevrangebyscore(GRAD_KEY, "+inf", WHALE_GRAD_USD, withscores=True)
        return [{"address": addr, "pnl_30d": score} for addr, score in entries]
    except Exception as exc:
        log.debug("Redis graduated whales fetch: %s", exc)
        return []


def get_recent_whales(limit: int = 200) -> list[dict]:
    try:
        r = redis_sync.from_url(_url(), decode_responses=True, socket_connect_timeout=2)
        cutoff = time.time() - TTL
        keys = r.zrevrangebyscore(WHALE_KEY, "+inf", cutoff, start=0, num=limit)
        if not keys:
            return []
        values = r.hmget(WHALE_DATA_KEY, keys)
        return [json.loads(v) for v in values if v]
    except Exception as exc:
        log.debug("Redis whale fetch: %s", exc)
        return []
