"""Gateway integration tests: limiter in the request path, headers, Redis failure."""
import time

import fakeredis
import httpx
import pytest
from fastapi.testclient import TestClient

from app.backend.main import app as backend_app
from app.gateway import main as gateway_main


def make_client(monkeypatch, *, capacity=3, rate=0.001, fail_mode="open", server=None):
    monkeypatch.setenv("BUCKET_CAPACITY", str(capacity))
    monkeypatch.setenv("BUCKET_REFILL_PER_SEC", str(rate))
    monkeypatch.setenv("RATE_LIMIT_FAIL_MODE", fail_mode)
    server = server or fakeredis.FakeServer()
    monkeypatch.setattr(gateway_main, "create_redis",
                        lambda _s: fakeredis.FakeAsyncRedis(server=server, decode_responses=True))
    monkeypatch.setattr(gateway_main, "create_http_client",
                        lambda _s: httpx.AsyncClient(transport=httpx.ASGITransport(app=backend_app),
                                                     base_url="http://backend"))
    return TestClient(gateway_main.app)


def test_allowed_requests_are_forwarded_with_metadata(monkeypatch):
    with make_client(monkeypatch, capacity=3) as c:
        r = c.get("/api/ping")
    assert r.status_code == 200
    assert r.json() == {"message": "pong"}
    assert r.headers["X-RateLimit-Limit"] == "3"
    assert r.headers["X-RateLimit-Remaining"] == "2"
    assert "X-RateLimit-Reset" in r.headers
    assert "Retry-After" not in r.headers


def test_exhausted_bucket_returns_429(monkeypatch):
    with make_client(monkeypatch, capacity=2) as c:
        codes = [c.get("/api/ping").status_code for _ in range(4)]
        blocked = c.get("/api/ping")
    assert codes == [200, 200, 429, 429]
    assert blocked.json()["error"] == "rate limit exceeded"
    assert int(blocked.headers["Retry-After"]) >= 1
    assert blocked.headers["X-RateLimit-Remaining"] == "0"


def test_backend_errors_pass_through_and_still_count(monkeypatch):
    with make_client(monkeypatch, capacity=2) as c:
        r = c.get("/api/fail/404")
        assert r.status_code == 404
        assert r.headers["X-RateLimit-Remaining"] == "1"


def test_compressed_upstream_responses_are_returned_decoded_without_a_stale_header(monkeypatch):
    """Regression (found in the Phase II review): a gzip-encoding upstream used to make the gateway send a
    decoded body labelled `Content-Encoding: gzip`, which clients cannot read."""
    import gzip

    def upstream(request):
        return httpx.Response(200, content=gzip.compress(b'{"ok": true}'),
                              headers={"content-encoding": "gzip", "content-type": "application/json"})
    server = fakeredis.FakeServer()
    monkeypatch.setenv("BUCKET_CAPACITY", "3")
    monkeypatch.setattr(gateway_main, "create_redis",
                        lambda _s: fakeredis.FakeAsyncRedis(server=server, decode_responses=True))
    monkeypatch.setattr(gateway_main, "create_http_client",
                        lambda _s: httpx.AsyncClient(transport=httpx.MockTransport(upstream), base_url="http://u"))
    with TestClient(gateway_main.app) as c:
        r = c.get("/api/anything")
    assert r.status_code == 200 and r.json() == {"ok": True}
    assert "content-encoding" not in r.headers


def test_different_api_keys_get_separate_buckets(monkeypatch):
    with make_client(monkeypatch, capacity=1) as c:
        assert c.get("/api/ping", headers={"X-API-Key": "alice"}).status_code == 200
        assert c.get("/api/ping", headers={"X-API-Key": "alice"}).status_code == 429
        assert c.get("/api/ping", headers={"X-API-Key": "bob"}).status_code == 200


def test_x_forwarded_for_cannot_be_used_to_dodge_limit(monkeypatch):
    with make_client(monkeypatch, capacity=1) as c:
        assert c.get("/api/ping", headers={"X-Forwarded-For": "1.1.1.1"}).status_code == 200
        assert c.get("/api/ping", headers={"X-Forwarded-For": "2.2.2.2"}).status_code == 429


def test_tokens_refill_in_real_time(monkeypatch):
    with make_client(monkeypatch, capacity=1, rate=20) as c:   # 1 token per 50 ms
        assert c.get("/api/ping").status_code == 200
        assert c.get("/api/ping").status_code == 429
        time.sleep(0.15)
        assert c.get("/api/ping").status_code == 200


def test_redis_down_fail_open(monkeypatch):
    server = fakeredis.FakeServer()
    server.connected = False
    with make_client(monkeypatch, fail_mode="open", server=server) as c:
        r = c.get("/api/ping")
    assert r.status_code == 200
    assert r.headers["X-RateLimit-Status"] == "bypassed"
    assert "X-RateLimit-Remaining" not in r.headers


def test_redis_down_fail_closed(monkeypatch):
    server = fakeredis.FakeServer()
    server.connected = False
    with make_client(monkeypatch, fail_mode="closed", server=server) as c:
        r = c.get("/api/ping")
    assert r.status_code == 503
    assert r.headers["Retry-After"] == "5"


def test_invalid_fail_mode_is_rejected(monkeypatch):
    monkeypatch.setenv("RATE_LIMIT_FAIL_MODE", "maybe")
    from app.config import load_settings
    with pytest.raises(ValueError):
        load_settings()
