"""Test harness that runs the WHOLE system in one process, on a simulated clock.

  httpx client -> gateway app (real lifespan: policy cache, telemetry publisher, token bucket)
               -> demo backend (in-process ASGI)
  telemetry stream -> BehaviourWorker (behaviour engine + policy engine) -> policy table
  -> policy cache refresh -> next request is enforced with the new policy

Redis is `fakeredis` by default. Set REAL_REDIS_URL (must end in /15, a scratch database that
the tests flush) to run the same tests against a real Redis server.

Simulated time: the gateway's request timestamps and the token bucket's clock are driven by
`Flow.now_ms`, so minutes of traffic run in seconds. Event processing is real code, not mocks.
The worker, policy cache refresh and telemetry flush are driven explicitly once per simulated
second (`Flow.tick`), which is how the live loops behave at ~1 s granularity.
"""
import hashlib
import os
import time as real_time
from contextlib import asynccontextmanager

import fakeredis
import httpx
import redis.asyncio as redis

from app.backend.main import app as backend_app
from app.common.keys import TELEMETRY_STREAM
from app.common.redis_client import create_redis
from app.config import Settings
from app.gateway import main as gateway_main
from app.policy.params import PolicyParams
from app.worker.consumer import BehaviourWorker
from app.worker.params import ScoringParams

ENDPOINTS5 = ["/api/ping", "/api/items/1", "/api/users/1", "/api/orders", "/api/search"]
EPOCH_MS = 1_800_000_000_000


def cid(api_key: str) -> str:
    """Client id the gateway derives from an X-API-Key."""
    return "key:" + hashlib.sha256(api_key.encode()).hexdigest()[:16]


class _FakeTime:
    """Stands in for the `time` module inside app.gateway.main."""

    def __init__(self):
        self.now_ms = EPOCH_MS

    def time_ns(self):
        return self.now_ms * 1_000_000

    def __getattr__(self, name):
        return getattr(real_time, name)


class Flow:
    def __init__(self, app, worker, make_redis, clock: _FakeTime | None):
        self.app, self.worker, self.make_redis, self.clock = app, worker, make_redis, clock
        self.gw = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway")
        self.r = make_redis()                    # inspection client
        self.sent = 0
        self.statuses: dict[str, list[int]] = {}
        self.bystander = True

    # ---- traffic ---------------------------------------------------------------
    async def send(self, api_key: str, path: str):
        resp = await self.gw.get(path, headers={"X-API-Key": api_key})
        self.sent += 1
        self.statuses.setdefault(api_key, []).append(resp.status_code)
        return resp

    async def burst(self, api_key: str, paths: list[str]):
        """All requests issued concurrently."""
        import asyncio
        return await asyncio.gather(*[self.send(api_key, p) for p in paths])

    async def tick(self, *, worker=True):
        """End of one simulated second: publish telemetry, run the worker, refresh policy."""
        await self.app.state.telemetry.flush()
        if worker:
            while await self.worker.process_once(block_ms=None):
                pass
            await self.app.state.policy.refresh()
        if self.clock:
            self.clock.now_ms += 1000

    async def second(self, plan: dict[str, list[str]], *, worker=True):
        """plan: api_key -> list of paths sent concurrently during this simulated second."""
        results = {}
        for key, paths in plan.items():
            results[key] = await self.burst(key, paths)
        if self.bystander:
            win = (self.clock.now_ms // 1000) if self.clock else int(real_time.time())
            await self.send("bystander", ENDPOINTS5[win % 5])
        await self.tick(worker=worker)
        return results

    async def run(self, seconds: int, plan_fn, **kw):
        out = []
        for i in range(seconds):
            out.append(await self.second(plan_fn(i), **kw))
        return out

    # ---- inspection -------------------------------------------------------------
    async def policy(self, api_key: str) -> dict:
        return await self.r.hgetall(f"policy:{cid(api_key)}")

    async def state(self, api_key: str) -> str:
        return (await self.policy(api_key)).get("state", "NORMAL")

    async def transitions(self, api_key: str) -> list[tuple[str, str]]:
        c = cid(api_key)
        return [(f["from"], f["to"]) for _i, f in await self.r.xrange("policy:events") if f["client"] == c]

    async def enforced(self) -> dict:
        return await self.r.hgetall("policy:enforced")

    async def telemetry_events(self) -> list[dict]:
        return [f for _i, f in await self.r.xrange(TELEMETRY_STREAM)]

    async def close(self):
        await self.gw.aclose()
        await self.r.aclose()


def _redis_factory():
    url = os.environ.get("REAL_REDIS_URL")
    if url:
        if not url.rstrip("/").endswith("/15"):
            raise RuntimeError("REAL_REDIS_URL must point at scratch database 15 (it is flushed)")
        # the same factory the gateway uses in production (blocking pool), pointed at the scratch database
        return (lambda: create_redis(Settings(), url=url)), True
    server = fakeredis.FakeServer()
    return (lambda: fakeredis.FakeAsyncRedis(server=server, decode_responses=True)), False


@asynccontextmanager
async def running_system(monkeypatch, *, simulated_clock=True, capacity=20, refill=10,
                         scoring: ScoringParams | None = None, queue_size=10_000):
    monkeypatch.setenv("TELEMETRY_QUEUE_SIZE", str(queue_size))
    monkeypatch.setenv("BUCKET_CAPACITY", str(capacity))
    monkeypatch.setenv("BUCKET_REFILL_PER_SEC", str(refill))
    monkeypatch.setenv("POLICY_REFRESH_S", "0.05")
    make_redis, real = _redis_factory()
    if real:
        await make_redis().flushdb()
    clock = _FakeTime() if simulated_clock else None
    monkeypatch.setattr(gateway_main, "create_redis", lambda _s: make_redis())
    monkeypatch.setattr(gateway_main, "create_http_client",
                        lambda _s: httpx.AsyncClient(transport=httpx.ASGITransport(app=backend_app),
                                                     base_url="http://backend"))
    if clock:
        monkeypatch.setattr(gateway_main, "time", clock)
    app = gateway_main.app
    worker_redis = make_redis()
    worker = BehaviourWorker(worker_redis, scoring or ScoringParams(warmup_evals=10), PolicyParams())
    async with app.router.lifespan_context(app):
        if clock:   # the token bucket must use simulated time too (it normally uses Redis TIME)
            bucket, original = app.state.bucket, app.state.bucket.consume

            async def consume(client_id, cost=1, now_ms=None, multiplier=1.0):
                return await original(client_id, cost, clock.now_ms, multiplier)
            bucket.consume = consume
        await worker.setup()
        flow = Flow(app, worker, make_redis, clock)
        try:
            yield flow
        finally:
            await flow.close()
    await worker_redis.aclose()
