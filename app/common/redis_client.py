"""Redis connection helper (async)."""
import redis.asyncio as redis

from app.config import Settings


def create_redis(settings: Settings, url: str | None = None) -> redis.Redis:
    """One shared client per process.

    A BLOCKING pool is used on purpose. With the default pool, a burst of more simultaneous requests
    than the pool size raised "Too many connections", which the gateway treats as a Redis failure and
    (in fail-open mode) lets the request through UNLIMITED, i.e. the limiter failed exactly when a burst
    hit it. (Found by running the test suite against a real Redis.) With a blocking pool extra requests
    wait up to `redis_timeout_s` for a free connection; only a genuine timeout counts as a failure.
    """
    pool = redis.BlockingConnectionPool.from_url(
        url or settings.redis_url,
        max_connections=settings.redis_max_connections,
        timeout=settings.redis_timeout_s,            # how long to wait for a free connection
        decode_responses=True,                       # str instead of bytes everywhere
        socket_timeout=settings.redis_timeout_s,     # a dead Redis must not hang the request path
        socket_connect_timeout=settings.redis_timeout_s,
        # PING a pooled connection that has been idle >15 s before reusing it, so a connection the
        # server closed meanwhile is replaced instead of failing the first request that uses it.
        health_check_interval=15,
    )
    return redis.Redis(connection_pool=pool)


async def close_redis(client: redis.Redis) -> None:
    """Close the client and release every pooled connection."""
    await client.aclose()
    await client.connection_pool.disconnect()


async def ping(client: redis.Redis) -> bool:
    try:
        return bool(await client.ping())
    except redis.RedisError:
        return False
