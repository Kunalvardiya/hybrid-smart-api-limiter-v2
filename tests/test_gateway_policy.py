"""Gateway enforcement tests: the policy table in Redis changes how the token bucket is applied."""
import asyncio
import hashlib
import inspect
import time

import fakeredis
import httpx
import pytest
from fastapi.testclient import TestClient

from app.backend.main import app as backend_app
from app.common.keys import TELEMETRY_STREAM, bucket_key
from app.gateway import main as gateway_main
from app.gateway.policy_cache import PolicyCache
from app.policy.params import State
from app.policy.store import ENFORCED_HASH, format_enforced

KEY = {"X-API-Key": "alice"}
CLIENT = "key:" + hashlib.sha256(b"alice").hexdigest()[:16]


def make_client(monkeypatch, server, *, capacity=8):
    monkeypatch.setenv("BUCKET_CAPACITY", str(capacity))
    monkeypatch.setenv("BUCKET_REFILL_PER_SEC", "0.001")
    monkeypatch.setenv("POLICY_REFRESH_S", "0.05")
    monkeypatch.setattr(gateway_main, "create_redis",
                        lambda _s: fakeredis.FakeAsyncRedis(server=server, decode_responses=True))
    monkeypatch.setattr(gateway_main, "create_http_client",
                        lambda _s: httpx.AsyncClient(transport=httpx.ASGITransport(app=backend_app),
                                                     base_url="http://backend"))
    return TestClient(gateway_main.app)


def set_policy(server, client_id, state, multiplier, age_ms=0):
    r = fakeredis.FakeRedis(server=server, decode_responses=True)
    r.hset(ENFORCED_HASH, client_id,
           format_enforced(state, multiplier, int(time.time() * 1000) - age_ms))


def wait_until(predicate, timeout=3.0):
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_throttled_client_gets_a_scaled_down_bucket(monkeypatch):
    server = fakeredis.FakeServer()
    set_policy(server, CLIENT, State.THROTTLED, 0.25)
    with make_client(monkeypatch, server, capacity=8) as c:
        codes = [c.get("/api/ping", headers=KEY).status_code for _ in range(5)]
        last = c.get("/api/ping", headers=KEY)
    assert codes == [200, 200, 429, 429, 429]          # capacity 8 x 0.25 = 2
    assert last.headers["X-RateLimit-Limit"] == "2"
    assert last.headers["X-Policy-State"] == "THROTTLED"


def test_other_clients_are_unaffected_by_someone_elses_policy(monkeypatch):
    server = fakeredis.FakeServer()
    set_policy(server, CLIENT, State.THROTTLED, 0.25)
    with make_client(monkeypatch, server, capacity=8) as c:
        bob = [c.get("/api/ping", headers={"X-API-Key": "bob"}).status_code for _ in range(8)]
        r = c.get("/api/ping", headers={"X-API-Key": "bob"})
    assert bob == [200] * 8 and r.status_code == 429
    assert "X-Policy-State" not in r.headers


def test_monitored_client_is_observed_but_not_restricted(monkeypatch):
    server = fakeredis.FakeServer()
    set_policy(server, CLIENT, State.MONITORED, 1.0)
    with make_client(monkeypatch, server, capacity=4) as c:
        responses = [c.get("/api/ping", headers=KEY) for _ in range(4)]
    assert [r.status_code for r in responses] == [200] * 4
    assert responses[0].headers["X-Policy-State"] == "MONITORED"
    assert responses[0].headers["X-RateLimit-Limit"] == "4"


def test_blocked_client_is_rejected_immediately_without_touching_the_bucket(monkeypatch):
    server = fakeredis.FakeServer()
    set_policy(server, CLIENT, State.BLOCKED, 0.0)
    with make_client(monkeypatch, server) as c:
        r = c.get("/api/ping", headers=KEY)
        ok = c.get("/api/ping", headers={"X-API-Key": "bob"})
    assert r.status_code == 429 and r.headers["Retry-After"] == "60"
    assert r.headers["X-Policy-State"] == "BLOCKED" and "blocked" in r.json()["error"]
    assert ok.status_code == 200
    assert not fakeredis.FakeRedis(server=server).exists(bucket_key(CLIENT))   # no token consumed


def test_blocked_requests_still_appear_in_telemetry_as_demand(monkeypatch):
    server = fakeredis.FakeServer()
    set_policy(server, CLIENT, State.BLOCKED, 0.0)
    with make_client(monkeypatch, server) as c:
        c.get("/api/ping", headers=KEY)
        assert wait_until(lambda: fakeredis.FakeRedis(server=server).xlen(TELEMETRY_STREAM) >= 1)
    e = fakeredis.FakeRedis(server=server, decode_responses=True).xrange(TELEMETRY_STREAM)[0][1]
    assert e["outcome"] == "blocked" and e["up_status"] == "0" and e["status"] == "429"
    assert e["client"] == CLIENT                      # keeps counting toward this client's score


def test_policy_changes_are_picked_up_without_restart_and_can_be_lifted(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server) as c:
        assert c.get("/api/ping", headers=KEY).status_code == 200
        set_policy(server, CLIENT, State.BLOCKED, 0.0)
        assert wait_until(lambda: c.get("/api/ping", headers=KEY).headers.get("X-Policy-State") == "BLOCKED")
        fakeredis.FakeRedis(server=server).hdel(ENFORCED_HASH, CLIENT)
        assert wait_until(lambda: "X-Policy-State" not in c.get("/api/ping", headers=KEY).headers)


def test_stale_policy_entries_are_ignored_because_the_worker_is_presumed_dead(monkeypatch):
    server = fakeredis.FakeServer()
    set_policy(server, CLIENT, State.BLOCKED, 0.0, age_ms=10 * 60 * 1000)     # 10 minutes old
    with make_client(monkeypatch, server) as c:
        r = c.get("/api/ping", headers=KEY)
        stats = c.get("/healthz").json()["policy"]
    assert r.status_code == 200 and "X-Policy-State" not in r.headers
    assert stats["ignored_stale"] == 1


# ---- PolicyCache unit tests ---------------------------------------------------------
def test_policy_lookup_is_synchronous_so_it_cannot_block_the_fast_path():
    assert not inspect.iscoroutinefunction(PolicyCache.get)


async def test_cache_keeps_last_policy_when_redis_fails_then_gives_up_after_max_age():
    server = fakeredis.FakeServer()
    client = fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
    await client.hset(ENFORCED_HASH, "ip:1.1.1.1",
                      format_enforced(State.BLOCKED, 0.0, int(time.time() * 1000)))
    cache = PolicyCache(client, refresh_s=0.05, max_age_s=0.3)
    await cache.refresh()
    assert cache.get("ip:1.1.1.1").state == State.BLOCKED
    assert cache.get("ip:9.9.9.9").state == State.NORMAL

    server.connected = False
    with pytest.raises(Exception):
        await cache.refresh()
    assert cache.get("ip:1.1.1.1").state == State.BLOCKED          # within max age: keep enforcing
    await asyncio.sleep(0.4)
    assert cache.get("ip:1.1.1.1").state == State.NORMAL            # too old: fall back to plain bucket


async def test_cache_ignores_malformed_entries():
    client = fakeredis.FakeAsyncRedis(decode_responses=True)
    await client.hset(ENFORCED_HASH, "ip:2.2.2.2", "garbage")
    cache = PolicyCache(client)
    await cache.refresh()
    assert cache.get("ip:2.2.2.2").state == State.NORMAL
