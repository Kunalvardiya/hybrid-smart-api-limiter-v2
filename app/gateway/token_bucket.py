"""Deterministic token-bucket limiter backed by Redis.

How it works (beginner version):
  * Every client owns a "bucket" that holds up to `capacity` tokens.
  * Tokens drip back in at `refill_per_sec` tokens per second.
  * Each request spends `cost` tokens. If the bucket has enough, the request is
    allowed; otherwise it is rejected and we tell the client how long to wait.

Why a Lua script: "read tokens -> compute -> write tokens" must happen as one
indivisible step. Redis runs a Lua script without interleaving other commands,
so two simultaneous requests can never spend the same token.

Time comes from the Redis server clock (TIME), so several gateway instances
share one clock and never disagree because of local clock drift.
"""
from dataclasses import dataclass

import redis.asyncio as redis

from app.common.keys import bucket_key

# KEYS[1] = bucket key
# ARGV    = capacity, refill_per_sec, cost, [now_ms override - tests only]
# Returns = {allowed(0/1), remaining_tokens(int), retry_after_ms, ms_until_full}
_LUA = """
local cap  = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local now
if ARGV[4] and ARGV[4] ~= '' then
  now = tonumber(ARGV[4])
else
  local t = redis.call('TIME')
  now = t[1] * 1000 + math.floor(t[2] / 1000)
end

local data   = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then
  tokens = cap
  ts = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(cap, tokens + elapsed * rate / 1000)

local allowed = 0
local retry_ms = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_ms = math.ceil((cost - tokens) / rate * 1000)
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
-- Idle buckets expire; an expired bucket is equivalent to a full one.
redis.call('PEXPIRE', KEYS[1], math.ceil(cap / rate * 1000) * 2 + 1000)

local full_ms = math.ceil((cap - tokens) / rate * 1000)
return {allowed, math.floor(tokens), retry_ms, full_ms}
"""


@dataclass(frozen=True)
class BucketResult:
    allowed: bool
    limit: int              # bucket capacity
    remaining: int          # whole tokens left after this request
    retry_after_s: int      # seconds to wait before retrying (0 if allowed)
    reset_s: int            # seconds until the bucket is full again


class TokenBucket:
    def __init__(self, client: redis.Redis, capacity: int, refill_per_sec: float):
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        if refill_per_sec <= 0:
            raise ValueError("refill_per_sec must be > 0")
        self._capacity = capacity
        self._rate = refill_per_sec
        self._script = client.register_script(_LUA)

    @property
    def capacity(self) -> int:
        return self._capacity

    async def consume(self, client_id: str, cost: int = 1, now_ms: int | None = None,
                      multiplier: float = 1.0) -> BucketResult:
        """Try to spend `cost` tokens. Raises redis.RedisError if Redis is unavailable.

        `multiplier` (0 < m <= 1) scales capacity and refill rate: this is how a
        THROTTLED client gets a smaller bucket. The effective capacity is never below 1.
        `now_ms` overrides the Redis clock; it exists so tests can control time.
        """
        if not 0 < multiplier <= 1:
            raise ValueError("multiplier must be in (0, 1]")
        capacity = max(1, int(self._capacity * multiplier))
        rate = self._rate * multiplier
        args = [capacity, rate, cost, "" if now_ms is None else now_ms]
        allowed, remaining, retry_ms, full_ms = await self._script(
            keys=[bucket_key(client_id)], args=args
        )
        return BucketResult(
            allowed=bool(allowed),
            limit=capacity,
            remaining=int(remaining),
            retry_after_s=-(-int(retry_ms) // 1000),   # ceil to whole seconds
            reset_s=-(-int(full_ms) // 1000),
        )
