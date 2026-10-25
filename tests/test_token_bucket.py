"""Token bucket tests: allow, exhaust, refill, concurrency, bad config.

Time is controlled with `now_ms`, so refill tests are exact and never sleep.
These run against fakeredis (with Lua via `lupa`). The same tests can be pointed
at a real Redis with: REAL_REDIS_URL=redis://:password@localhost:6379/0 pytest
"""
import asyncio
import os

import fakeredis
import pytest
import redis.asyncio as redis

from app.gateway.token_bucket import TokenBucket


@pytest.fixture
async def client():
    url = os.environ.get("REAL_REDIS_URL")
    c = redis.Redis.from_url(url, decode_responses=True) if url else \
        fakeredis.FakeAsyncRedis(decode_responses=True)
    # Only touch this suite's own keys, so a shared real Redis is never flushed.
    stale = [k async for k in c.scan_iter("bucket:test*")]
    if stale:
        await c.delete(*stale)
    yield c
    await c.aclose()


T0 = 1_000_000  # arbitrary fixed "now" in ms


async def test_requests_allowed_until_capacity(client):
    tb = TokenBucket(client, capacity=3, refill_per_sec=1)
    results = [await tb.consume("test-a", now_ms=T0) for _ in range(3)]
    assert [r.allowed for r in results] == [True, True, True]
    assert [r.remaining for r in results] == [2, 1, 0]
    assert all(r.limit == 3 for r in results)


async def test_exhausted_bucket_rejects_with_retry_after(client):
    tb = TokenBucket(client, capacity=2, refill_per_sec=1)
    await tb.consume("test-a", now_ms=T0)
    await tb.consume("test-a", now_ms=T0)
    r = await tb.consume("test-a", now_ms=T0)
    assert not r.allowed
    assert r.remaining == 0
    assert r.retry_after_s == 1          # needs 1 token at 1 token/s
    assert r.reset_s == 2                # empty -> full takes 2 s


async def test_rejected_requests_do_not_consume_tokens(client):
    tb = TokenBucket(client, capacity=1, refill_per_sec=1)
    await tb.consume("test-a", now_ms=T0)
    for _ in range(5):
        assert not (await tb.consume("test-a", now_ms=T0)).allowed
    # exactly one second later there must be exactly one token
    assert (await tb.consume("test-a", now_ms=T0 + 1000)).allowed


async def test_refill_over_time(client):
    tb = TokenBucket(client, capacity=4, refill_per_sec=2)
    for _ in range(4):
        await tb.consume("test-a", now_ms=T0)
    assert not (await tb.consume("test-a", now_ms=T0)).allowed
    # 1 second at 2 tokens/s -> 2 tokens back
    assert (await tb.consume("test-a", now_ms=T0 + 1000)).allowed
    assert (await tb.consume("test-a", now_ms=T0 + 1000)).allowed
    assert not (await tb.consume("test-a", now_ms=T0 + 1000)).allowed


async def test_refill_is_capped_at_capacity(client):
    tb = TokenBucket(client, capacity=3, refill_per_sec=10)
    await tb.consume("test-a", now_ms=T0)
    r = await tb.consume("test-a", now_ms=T0 + 3_600_000)   # an hour later
    assert r.remaining == 2              # full (3) minus this request, not more


async def test_clients_have_independent_buckets(client):
    tb = TokenBucket(client, capacity=1, refill_per_sec=1)
    assert (await tb.consume("test-a", now_ms=T0)).allowed
    assert not (await tb.consume("test-a", now_ms=T0)).allowed
    assert (await tb.consume("test-b", now_ms=T0)).allowed


async def test_concurrent_requests_never_overspend(client):
    """100 simultaneous requests, capacity 20, ~no refill: exactly 20 may pass."""
    tb = TokenBucket(client, capacity=20, refill_per_sec=0.001)
    results = await asyncio.gather(*[tb.consume("test-race", now_ms=T0) for _ in range(100)])
    assert sum(r.allowed for r in results) == 20


async def test_concurrent_requests_with_real_clock(client):
    """Same race using the Redis server clock (the production code path)."""
    tb = TokenBucket(client, capacity=10, refill_per_sec=0.001)
    results = await asyncio.gather(*[tb.consume("test-race2") for _ in range(50)])
    assert sum(r.allowed for r in results) == 10


async def test_multiplier_scales_capacity_and_refill(client):
    tb = TokenBucket(client, capacity=8, refill_per_sec=4)
    first = await tb.consume("test-m", now_ms=T0, multiplier=0.25)
    assert first.limit == 2 and first.remaining == 1               # 8 x 0.25
    await tb.consume("test-m", now_ms=T0, multiplier=0.25)
    assert not (await tb.consume("test-m", now_ms=T0, multiplier=0.25)).allowed
    # refill is 4 x 0.25 = 1 token/s
    assert not (await tb.consume("test-m", now_ms=T0 + 500, multiplier=0.25)).allowed
    assert (await tb.consume("test-m", now_ms=T0 + 1000, multiplier=0.25)).allowed


async def test_multiplier_never_gives_capacity_below_one(client):
    tb = TokenBucket(client, capacity=3, refill_per_sec=1)
    assert (await tb.consume("test-m2", now_ms=T0, multiplier=0.1)).limit == 1


async def test_restoring_the_multiplier_restores_capacity_smoothly(client):
    tb = TokenBucket(client, capacity=10, refill_per_sec=10)
    await tb.consume("test-m3", now_ms=T0, multiplier=0.2)         # stored tokens capped at 2
    r = await tb.consume("test-m3", now_ms=T0 + 1000, multiplier=1.0)
    assert r.allowed and r.remaining == 9                          # refilled to the full 10 in 1 s, minus this one


@pytest.mark.parametrize("bad", [0, -0.5, 1.5])
async def test_invalid_multiplier_rejected(client, bad):
    with pytest.raises(ValueError):
        await TokenBucket(client, 5, 1).consume("test-x", multiplier=bad)


def test_invalid_configuration_rejected():
    with pytest.raises(ValueError):
        TokenBucket(fakeredis.FakeAsyncRedis(), capacity=0, refill_per_sec=1)
    with pytest.raises(ValueError):
        TokenBucket(fakeredis.FakeAsyncRedis(), capacity=1, refill_per_sec=0)
