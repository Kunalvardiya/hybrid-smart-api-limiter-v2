"""Telemetry tests: event contents, windows, privacy, loss reporting."""
import asyncio
import inspect
import time

import fakeredis
import httpx
import pytest
from fastapi.testclient import TestClient

from app.backend.main import app as backend_app
from app.common.keys import TELEMETRY_STREAM
from app.config import load_settings
from app.gateway import main as gateway_main
from app.gateway.telemetry import TelemetryPublisher, build_event, normalize_endpoint


def make_client(monkeypatch, server, *, capacity=100, env=None):
    monkeypatch.setenv("BUCKET_CAPACITY", str(capacity))
    monkeypatch.setenv("BUCKET_REFILL_PER_SEC", "0.001")
    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)
    monkeypatch.setattr(gateway_main, "create_redis",
                        lambda _s: fakeredis.FakeAsyncRedis(server=server, decode_responses=True))
    monkeypatch.setattr(gateway_main, "create_http_client",
                        lambda _s: httpx.AsyncClient(transport=httpx.ASGITransport(app=backend_app),
                                                     base_url="http://backend"))
    return TestClient(gateway_main.app)


def read_stream(server):
    r = fakeredis.FakeRedis(server=server, decode_responses=True)
    return [fields for _id, fields in r.xrange(TELEMETRY_STREAM)]


def wait_for_events(server, n, timeout=3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        events = read_stream(server)
        if len(events) >= n:
            return events
        time.sleep(0.02)
    return read_stream(server)


# ---- endpoint normalisation ------------------------------------------------
@pytest.mark.parametrize("path,expected", [
    ("/api/ping", "/api/ping"),
    ("/api/items/42", "/api/items/{id}"),
    ("/api/items/42?token=secret", "/api/items/{id}"),
    ("/api/users/123e4567-e89b-12d3-a456-426614174000/orders", "/api/users/{id}/orders"),
    ("/api/blob/deadbeefdeadbeefdeadbeef", "/api/blob/{id}"),
])
def test_normalize_endpoint(path, expected):
    assert normalize_endpoint(path) == expected


# ---- events recorded through the gateway -------------------------------------
def test_allowed_request_event(monkeypatch):
    server = fakeredis.FakeServer()
    before = int(time.time() * 1000)
    with make_client(monkeypatch, server) as c:
        c.get("/api/items/7", headers={"X-API-Key": "alice"})
        events = wait_for_events(server, 1)
    after = int(time.time() * 1000)

    assert len(events) == 1
    e = events[0]
    assert e["v"] == "1" and e["type"] == "req"
    assert e["client"].startswith("key:") and len(e["client"]) == 4 + 16
    assert before <= int(e["ts"]) <= after
    assert int(e["win"]) == int(e["ts"]) // 1000          # 1-second window id
    assert e["method"] == "GET" and e["endpoint"] == "/api/items/{id}"
    assert e["status"] == "200" and e["up_status"] == "200" and e["outcome"] == "allowed"
    assert int(e["latency_ms"]) >= 0


def test_upstream_4xx_is_recorded(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server) as c:
        c.get("/api/fail/404")
        e = wait_for_events(server, 1)[0]
    assert e["status"] == "404" and e["up_status"] == "404" and e["outcome"] == "allowed"


def test_limited_request_is_recorded_but_not_as_upstream_failure(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server, capacity=1) as c:
        c.get("/api/ping")
        c.get("/api/ping")
        events = wait_for_events(server, 2)
    assert [e["outcome"] for e in events] == ["allowed", "limited"]
    limited = events[1]
    assert limited["status"] == "429"
    assert limited["up_status"] == "0"          # must NOT count toward the 4xx failure rate F


def test_events_in_same_second_share_a_window_id(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server) as c:
        for _ in range(5):
            c.get("/api/ping")
        events = wait_for_events(server, 5)
    wins = {int(e["win"]) for e in events}
    assert all(int(e["ts"]) // 1000 == int(e["win"]) for e in events)
    assert len(wins) <= 2                       # 5 fast requests span at most a second boundary


def test_no_secrets_in_telemetry(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server) as c:
        c.post("/api/items/1?token=QUERYSECRET",
               headers={"X-API-Key": "APIKEYSECRET", "Authorization": "Bearer AUTHSECRET",
                        "Cookie": "session=COOKIESECRET"},
               content=b'{"password": "BODYSECRET"}')
        events = wait_for_events(server, 1)
    blob = " ".join(str(v) for e in events for v in list(e.keys()) + list(e.values()))
    for secret in ("QUERYSECRET", "APIKEYSECRET", "AUTHSECRET", "COOKIESECRET", "BODYSECRET"):
        assert secret not in blob


def test_telemetry_breakage_does_not_affect_requests(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server, env={"TELEMETRY_QUEUE_SIZE": "1"}) as c:
        codes = [c.get("/api/ping").status_code for _ in range(20)]
    assert codes == [200] * 20


def test_healthz_exposes_telemetry_counters(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server) as c:
        body = c.get("/healthz").json()
    assert set(body["telemetry"]) == {"queued", "published", "lost", "unreported_gap"}


# ---- client identity ---------------------------------------------------------
def test_unrecognised_key_falls_back_to_ip_identity(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server, env={"ALLOWED_API_KEYS": "good-key"}) as c:
        c.get("/api/ping", headers={"X-API-Key": "good-key"})
        c.get("/api/ping", headers={"X-API-Key": "made-up-1"})
        c.get("/api/ping", headers={"X-API-Key": "made-up-2"})
        events = wait_for_events(server, 3)
    clients = [e["client"] for e in events]
    assert clients[0].startswith("key:")
    assert clients[1].startswith("ip:") and clients[1] == clients[2]


def test_allowlist_parsing_hashes_keys():
    import os
    os.environ["ALLOWED_API_KEYS"] = " a , b ,"
    try:
        s = load_settings()
    finally:
        del os.environ["ALLOWED_API_KEYS"]
    assert len(s.allowed_key_hashes) == 2
    assert "a" not in s.allowed_key_hashes


# ---- heartbeat ticks ---------------------------------------------------------------
def test_gateway_emits_ticks_while_idle_and_they_are_not_counted_as_requests(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server, env={"TELEMETRY_TICK_S": "0.05"}) as c:
        events = wait_for_events(server, 4)
        stats = c.get("/healthz").json()["telemetry"]
    assert len(events) >= 4 and {e["type"] for e in events} == {"tick"}
    assert all(int(e["win"]) == int(e["ts"]) // 1000 for e in events)
    assert stats["published"] == 0 and stats["lost"] == 0         # ticks are not request telemetry


async def test_lost_ticks_never_create_gap_reports():
    server = fakeredis.FakeServer()
    client = fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
    p = TelemetryPublisher(client, tick_s=0.02)
    p.start()
    server.connected = False
    await asyncio.sleep(0.2)                     # ticks are generated and fail to publish
    server.connected = True
    await asyncio.sleep(0.2)
    await p.stop()
    types = {e["type"] for e in read_stream(server)}
    assert "gap" not in types and p.lost == 0


# ---- publisher: loss must be reported, never silent ----------------------------
def ev(i):
    return build_event(client="ip:1.2.3.4", method="GET", path="/api/ping", ts_ms=1_000_000 + i,
                       status=200, up_status=200, outcome="allowed", latency_ms=1)


def test_record_is_synchronous_and_never_raises():
    assert not inspect.iscoroutinefunction(TelemetryPublisher.record)
    p = TelemetryPublisher(fakeredis.FakeAsyncRedis(), queue_size=1)
    assert p.record(ev(0)) is True
    assert p.record(ev(1)) is False             # full -> dropped, no exception


async def test_queue_overflow_is_reported_as_gap():
    server = fakeredis.FakeServer()
    client = fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
    p = TelemetryPublisher(client, queue_size=2)
    for i in range(5):                          # publisher not started yet: 2 fit, 3 dropped
        p.record(ev(i))
    assert p.lost == 3
    p.start()
    await p.flush()
    await p.stop()

    events = read_stream(server)
    gaps = [e for e in events if e["type"] == "gap"]
    assert sum(e["type"] == "req" for e in events) == 2
    assert len(gaps) == 1 and gaps[0]["lost"] == "3"
    assert gaps[0]["from_ts"] == str(1_000_002) and gaps[0]["to_ts"] == str(1_000_004)
    assert p.stats()["unreported_gap"] == 0


async def test_redis_outage_is_reported_as_gap_after_recovery():
    server = fakeredis.FakeServer()
    client = fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
    p = TelemetryPublisher(client)
    p.start()

    server.connected = False
    for i in range(3):
        p.record(ev(i))
    await p.flush()
    assert p.lost == 3 and p.published == 0

    server.connected = True
    p.record(ev(10))
    await p.flush()
    await p.stop()

    events = read_stream(server)
    gap = next(e for e in events if e["type"] == "gap")
    assert gap["lost"] == "3"
    assert gap["from_ts"] == str(1_000_000) and gap["to_ts"] == str(1_000_002)
    assert sum(e["type"] == "req" for e in events) == 1


async def test_stream_is_trimmed_to_maxlen():
    server = fakeredis.FakeServer()
    client = fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
    # Approximate trimming (MAXLEN ~) only drops whole internal nodes of ~100
    # entries, so the stream may exceed maxlen slightly but cannot grow unbounded.
    p = TelemetryPublisher(client, maxlen=100, queue_size=1000)
    p.start()
    for i in range(1000):
        p.record(ev(i))
    await p.flush()
    await p.stop()
    assert await client.xlen(TELEMETRY_STREAM) < 1000
