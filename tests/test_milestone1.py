"""Milestone 1 tests: config, health endpoints, Redis connectivity.

Uses fakeredis, so no real Redis is needed for these.
"""
import fakeredis
from fastapi.testclient import TestClient

from app.backend.main import app as backend_app
from app.config import Settings, load_settings
from app.gateway import main as gateway_main


def test_redis_url_without_password():
    assert Settings().redis_url == "redis://localhost:6379/0"


def test_redis_url_encodes_special_characters():
    url = Settings(redis_password="p@ss/w:rd", redis_host="redis").redis_url
    assert url == "redis://:p%40ss%2Fw%3Ard@redis:6379/0"


async def test_redis_client_uses_a_blocking_pool_so_bursts_wait_instead_of_failing_open():
    """Regression (found on a real Redis): the default pool raised 'Too many connections' on bursts."""
    import redis.asyncio as redis
    from app.common.redis_client import create_redis
    client = create_redis(Settings(redis_max_connections=7, redis_timeout_s=0.3))
    pool = client.connection_pool
    assert isinstance(pool, redis.BlockingConnectionPool)
    assert pool.max_connections == 7 and pool.timeout == 0.3
    assert Settings().redis_max_connections >= 50
    await client.aclose()


def test_load_settings_reads_environment(monkeypatch):
    monkeypatch.setenv("REDIS_HOST", "example")
    monkeypatch.setenv("BUCKET_CAPACITY", "7")
    s = load_settings()
    assert s.redis_host == "example" and s.bucket_capacity == 7


def test_gateway_healthz_ok_with_redis(monkeypatch):
    monkeypatch.setattr(gateway_main, "create_redis",
                        lambda _s: fakeredis.FakeAsyncRedis(decode_responses=True))
    with TestClient(gateway_main.app) as client:
        r = client.get("/healthz")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok" and body["redis"] is True
    assert "telemetry" in body


def test_gateway_healthz_degraded_when_redis_down(monkeypatch):
    server = fakeredis.FakeServer()
    server.connected = False
    monkeypatch.setattr(gateway_main, "create_redis",
                        lambda _s: fakeredis.FakeAsyncRedis(server=server, decode_responses=True))
    with TestClient(gateway_main.app) as client:
        r = client.get("/healthz")
    assert r.status_code == 503
    assert r.json()["redis"] is False


def test_backend_endpoints():
    client = TestClient(backend_app)
    assert client.get("/api/ping").json() == {"message": "pong"}
    assert client.get("/api/items/3").json()["id"] == 3
    assert client.get("/api/fail/404").status_code == 404
    assert client.get("/api/fail/200").status_code == 400
