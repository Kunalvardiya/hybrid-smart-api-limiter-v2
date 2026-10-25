"""Tests for the read-only admin API and dashboard page (app/admin/api.py)."""
import hashlib
import time

import fakeredis
import httpx
import pytest
from fastapi.testclient import TestClient

from app.backend.main import app as backend_app
from app.common.keys import TELEMETRY_STREAM, policy_key
from app.gateway import main as gateway_main
from app.policy.params import State
from app.policy.store import ENFORCED_HASH, POLICY_EVENTS_STREAM, format_enforced
from app.worker.consumer import BehaviourWorker
from app.worker.engine import ScoreRecord

TOKEN = "test-admin-token"
H = {"X-Admin-Token": TOKEN}


def cid(key):
    return "key:" + hashlib.sha256(key.encode()).hexdigest()[:16]


def make_client(monkeypatch, server, *, token=TOKEN, capacity=3):
    if token:
        monkeypatch.setenv("ADMIN_TOKEN", token)
    else:
        monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    monkeypatch.setenv("BUCKET_CAPACITY", str(capacity))
    monkeypatch.setenv("BUCKET_REFILL_PER_SEC", "0.001")
    monkeypatch.setenv("POLICY_REFRESH_S", "0.05")
    monkeypatch.setattr(gateway_main, "create_redis",
                        lambda _s: fakeredis.FakeAsyncRedis(server=server, decode_responses=True))
    monkeypatch.setattr(gateway_main, "create_http_client",
                        lambda _s: httpx.AsyncClient(transport=httpx.ASGITransport(app=backend_app),
                                                     base_url="http://backend"))
    return TestClient(gateway_main.app)


def sync(server):
    return fakeredis.FakeRedis(server=server, decode_responses=True)


def wait_for(predicate, timeout=3.0):
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(0.02)
    return False


# ---- access control ----------------------------------------------------------------------------
@pytest.mark.parametrize("path", ["/admin/overview", "/admin/clients", "/admin/events"])
def test_admin_api_is_disabled_without_a_configured_token(monkeypatch, path):
    with make_client(monkeypatch, fakeredis.FakeServer(), token="") as c:
        r = c.get(path, headers=H)
    assert r.status_code == 503 and "disabled" in r.json()["detail"]


@pytest.mark.parametrize("path", ["/admin/overview", "/admin/clients", "/admin/events"])
def test_admin_api_rejects_missing_or_wrong_token(monkeypatch, path):
    with make_client(monkeypatch, fakeredis.FakeServer()) as c:
        assert c.get(path).status_code == 401
        assert c.get(path, headers={"X-Admin-Token": "nope"}).status_code == 401
        assert c.get(path, headers={"X-Admin-Token": TOKEN + "x"}).status_code == 401
        assert c.get(path + "?token=" + TOKEN).status_code == 401       # tokens in URLs are not accepted
        assert c.get(path, headers=H).status_code == 200


def test_query_parameters_are_validated(monkeypatch):
    with make_client(monkeypatch, fakeredis.FakeServer()) as c:
        assert c.get("/admin/clients?limit=0", headers=H).status_code == 422
        assert c.get("/admin/clients?limit=500", headers=H).status_code == 422
        assert c.get("/admin/overview?seconds=1", headers=H).status_code == 422


# ---- overview on an empty system ------------------------------------------------------------------
def test_overview_of_an_empty_system_shows_zeros_and_unknown_worker_not_fake_data(monkeypatch):
    with make_client(monkeypatch, fakeredis.FakeServer()) as c:
        d = c.get("/admin/overview", headers=H).json()
    w = d["window"]
    assert w["total"] == 0 and w["series"] == [] and w["outcomes"]["allowed"] == 0
    assert w["upstream"] == {"served": 0, "failures_4xx": 0, "failure_rate": None}   # None, not 0 %
    assert w["avg_rps"] == 0.0 and w["truncated"] is False
    assert d["health"]["redis"]["ok"] is True
    assert d["health"]["worker"]["status"] == "unknown"
    assert d["enforced"] == {"MONITORED": 0, "THROTTLED": 0, "BLOCKED": 0}


# ---- overview counts match what was actually sent ---------------------------------------------------
def test_overview_counts_match_the_requests_that_were_sent(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server, capacity=3) as c:
        sync(server).hset(ENFORCED_HASH, cid("evil"),
                          format_enforced(State.BLOCKED, 0.0, int(time.time() * 1000)))
        assert wait_for(lambda: c.get("/healthz").json()["policy"]["entries"] == 1)
        for _ in range(5):                                    # capacity 3: 3 allowed, 2 limited
            c.get("/api/ping", headers={"X-API-Key": "alice"})
        c.get("/api/fail/404", headers={"X-API-Key": "bob"})   # allowed, upstream 404
        c.get("/api/fail/401", headers={"X-API-Key": "bob"})   # allowed, upstream 401
        c.get("/api/ping", headers={"X-API-Key": "bob"})       # allowed, 200
        for _ in range(2):
            c.get("/api/ping", headers={"X-API-Key": "evil"})  # blocked
        assert wait_for(lambda: sync(server).xlen(TELEMETRY_STREAM) == 10)
        d = c.get("/admin/overview", headers=H).json()
    w = d["window"]
    assert w["total"] == 10
    assert w["outcomes"]["allowed"] == 6 and w["outcomes"]["limited"] == 2 and w["outcomes"]["blocked"] == 2
    assert w["restricted"] == 4
    # 6 allowed requests reached the API: 2 of them (404, 401) were upstream 4xx
    assert w["upstream"] == {"served": 6, "failures_4xx": 2, "failure_rate": pytest.approx(2 / 6)}
    assert w["active_clients"] == 3
    assert sum(s["total"] for s in w["series"]) == 10
    assert sum(s["restricted"] for s in w["series"]) == 4 and sum(s["failures"] for s in w["series"]) == 2
    assert w["peak_per_second"] == max(s["total"] for s in w["series"])
    assert d["enforced"]["BLOCKED"] == 1
    assert d["health"]["gateway"]["telemetry"]["published"] == 10


def test_window_is_marked_truncated_when_more_events_than_the_cap(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server, capacity=500) as c:
        for _ in range(150):
            c.get("/api/ping", headers={"X-API-Key": "alice"})
        assert wait_for(lambda: sync(server).xlen(TELEMETRY_STREAM) == 150)
        w = c.get("/admin/overview?cap=100", headers=H).json()["window"]
    assert w["truncated"] is True and w["events_scanned"] == 100 and w["total"] == 100


def test_reported_telemetry_gaps_are_surfaced(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server) as c:
        sync(server).xadd(TELEMETRY_STREAM, {"v": 1, "type": "gap", "ts": 1, "lost": 42, "from_ts": 1, "to_ts": 2})
        w = c.get("/admin/overview", headers=H).json()["window"]
    assert w["gap_events"] == 1 and w["events_lost_reported"] == 42 and w["total"] == 0


def test_events_outside_the_window_are_not_counted(monkeypatch):
    server = fakeredis.FakeServer()
    old = int((time.time() - 3600) * 1000)
    sync(server).xadd(TELEMETRY_STREAM, {"type": "req", "client": "x", "win": 1, "outcome": "allowed",
                                         "up_status": 200}, id=f"{old}-0")
    with make_client(monkeypatch, server) as c:
        assert c.get("/admin/overview?seconds=60", headers=H).json()["window"]["total"] == 0
        assert c.get("/admin/overview?seconds=600", headers=H).json()["window"]["total"] == 0


# ---- worker health ----------------------------------------------------------------------------------
async def test_worker_heartbeat_is_written_and_reported(monkeypatch):
    server = fakeredis.FakeServer()
    w = BehaviourWorker(fakeredis.FakeAsyncRedis(server=server, decode_responses=True))
    await w.setup()
    await w.process_once(block_ms=None)
    hb = sync(server).hgetall("worker:heartbeat")
    assert {"ts_ms", "processed", "clients_tracked", "late_events", "bad_events"} <= set(hb)
    assert 0 < sync(server).pttl("worker:heartbeat") <= 30_000
    with make_client(monkeypatch, server) as c:
        health = c.get("/admin/overview", headers=H).json()["health"]["worker"]
    assert health["status"] == "ok" and health["age_s"] < 5


def test_stale_heartbeat_is_reported_as_stale(monkeypatch):
    server = fakeredis.FakeServer()
    sync(server).hset("worker:heartbeat", mapping={"ts_ms": int((time.time() - 20) * 1000), "processed": 7})
    with make_client(monkeypatch, server) as c:
        health = c.get("/admin/overview", headers=H).json()["health"]["worker"]
    assert health["status"] == "stale" and health["age_s"] >= 19 and health["processed"] == 7


# ---- clients ------------------------------------------------------------------------------------------
def test_clients_endpoint_matches_what_the_worker_writes(monkeypatch):
    """Contract test: the exact fields ScoreRecord.to_fields() produces are parsed correctly."""
    server = fakeredis.FakeServer()
    rec = ScoreRecord("key:abc123", 500, "scored", 62.5, D=1.0, B=0.5, F=0.25, C=0.75, rate=12.0, peak=15,
                      baseline=4.0, obs_count=40, n_requests=120, n_served=100, n_4xx=25,
                      top_endpoint="/api/search", top_share=0.875, unavailable=("F",), notes=("n",))
    fields = rec.to_fields() | {"state": "THROTTLED", "state_since": "490", "multiplier": "0.25",
                                "state_reason": "r", "last_transition": "MONITORED->THROTTLED: score 62.5"}
    r = sync(server)
    r.hset(policy_key("key:abc123"), mapping=fields)
    r.hset(policy_key("ip:9.9.9.9"), mapping=ScoreRecord("ip:9.9.9.9", 500, "scored", 3.0, D=0, B=0, F=0, C=0).to_fields()
           | {"state": "NORMAL"})
    r.hset(policy_key("key:inc"), mapping=ScoreRecord("key:inc", 1, "incomplete", None).to_fields())
    with make_client(monkeypatch, server) as c:
        d = c.get("/admin/clients", headers=H).json()
    assert d["total_clients"] == 3
    top = d["clients"][0]
    assert top["client"] == "key:abc123" and top["state"] == "THROTTLED"          # most restricted first
    assert (top["score"], top["D"], top["B"], top["F"], top["C"]) == (62.5, 1.0, 0.5, 0.25, 0.75)
    assert (top["rate"], top["peak"], top["baseline"]) == (12.0, 15, 4.0)
    assert (top["n_requests"], top["n_served"], top["n_4xx"]) == (120, 100, 25)
    assert top["unavailable"] == ["F"] and top["top_endpoint"] == "/api/search"
    assert top["last_transition"].startswith("MONITORED->THROTTLED")
    incomplete = next(x for x in d["clients"] if x["client"] == "key:inc")
    assert incomplete["score"] is None and incomplete["status"] == "incomplete"     # no invented score


def test_clients_endpoint_limit_and_empty(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server) as c:
        assert c.get("/admin/clients", headers=H).json() == {"total_clients": 0, "returned": 0, "clients": []}
        r = sync(server)
        for i in range(5):
            r.hset(policy_key(f"ip:10.0.0.{i}"), mapping={"state": "NORMAL", "status": "scored", "score": str(i)})
        d = c.get("/admin/clients?limit=2", headers=H).json()
    assert d["total_clients"] == 5 and d["returned"] == 2
    assert [x["score"] for x in d["clients"]] == [4.0, 3.0]                         # highest score first


# ---- policy updates -----------------------------------------------------------------------------------
def test_events_endpoint_returns_newest_first(monkeypatch):
    server = fakeredis.FakeServer()
    r = sync(server)
    for i, (frm, to) in enumerate([("NORMAL", "MONITORED"), ("MONITORED", "THROTTLED"), ("THROTTLED", "BLOCKED")]):
        r.xadd(POLICY_EVENTS_STREAM, {"client": "key:a", "from": frm, "to": to, "win": 100 + i, "score": 30 + i,
                                      "D": 1, "B": 1, "F": 0, "C": 0, "reason": f"step {i}"})
    with make_client(monkeypatch, server) as c:
        ev = c.get("/admin/events?limit=2", headers=H).json()["events"]
        empty = make_client(monkeypatch, fakeredis.FakeServer())
    assert [e["to"] for e in ev] == ["BLOCKED", "THROTTLED"]
    assert ev[0]["win"] == 102 and ev[0]["reason"] == "step 2" and ev[0]["score"] == 32.0
    with empty as c2:
        assert c2.get("/admin/events", headers=H).json() == {"events": []}


# ---- failures ---------------------------------------------------------------------------------------
def test_redis_failure_gives_a_clear_503_not_fake_numbers(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server) as c:
        server.connected = False
        for path in ("/admin/overview", "/admin/clients", "/admin/events"):
            r = c.get(path, headers=H)
            assert r.status_code == 503 and r.json()["redis"] == {"ok": False}
        server.connected = True
        assert c.get("/admin/overview", headers=H).status_code == 200


# ---- privacy and the page itself ----------------------------------------------------------------------
def test_raw_api_keys_never_appear_in_admin_output(monkeypatch):
    server = fakeredis.FakeServer()
    with make_client(monkeypatch, server) as c:
        c.get("/api/ping", headers={"X-API-Key": "SUPERSECRETKEY"})
        assert wait_for(lambda: sync(server).xlen(TELEMETRY_STREAM) == 1)
        body = "".join(c.get(p, headers=H).text for p in ("/admin/overview", "/admin/clients", "/admin/events"))
    assert "SUPERSECRETKEY" not in body


def test_dashboard_page_is_served_without_data_and_without_external_resources(monkeypatch):
    with make_client(monkeypatch, fakeredis.FakeServer()) as c:
        r = c.get("/dashboard")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert r.headers["cache-control"] == "no-store"
    html = r.text
    assert "/admin/overview" in html and "X-Admin-Token" in html
    assert "src=\"http" not in html and "href=\"http" not in html and "cdn" not in html.lower()


def test_dashboard_works_without_a_token_configured_but_data_does_not(monkeypatch):
    with make_client(monkeypatch, fakeredis.FakeServer(), token="") as c:
        assert c.get("/dashboard").status_code == 200
        assert c.get("/admin/overview").status_code == 503
