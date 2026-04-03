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

FLOW_KEY         = "whaleleaks:flow"
WHALE_KEY        = "whaleleaks:whales"
MARKET_HIGH_KEY  = "whaleleaks:market_high"
TTL              = 30 * 24 * 3600   # 30 days in seconds
WEEK             = 7  * 24 * 3600   # 7 days in seconds


def _url() -> str:
    return getattr(config, "REDIS_URL", "redis://localhost:6379")


# ── Async writes (called from main asyncio loop) ──────────────────────────────

async def push_flow(flow: dict) -> None:
    try:
        r = aioredis.from_url(_url(), decode_responses=True)
        ts = time.time()
        async with r:
            await r.zadd(FLOW_KEY, {json.dumps(flow): ts})
            await r.zremrangebyscore(FLOW_KEY, 0, ts - TTL)
    except Exception as exc:
        log.debug("Redis flow push: %s", exc)


async def push_whale(alert: dict) -> None:
    try:
        r = aioredis.from_url(_url(), decode_responses=True)
        ts = time.time()
        async with r:
            await r.zadd(WHALE_KEY, {json.dumps(alert): ts})
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
        items = r.zrevrange(FLOW_KEY, 0, limit - 1)
        return [json.loads(i) for i in items]
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


def get_recent_whales(limit: int = 200) -> list[dict]:
    try:
        r = redis_sync.from_url(_url(), decode_responses=True, socket_connect_timeout=2)
        items = r.zrevrange(WHALE_KEY, 0, limit - 1)
        return [json.loads(i) for i in items]
    except Exception as exc:
        log.debug("Redis whale fetch: %s", exc)
        return []
