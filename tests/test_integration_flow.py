"""Full-flow integration tests (see flow_harness.py for how the system is assembled).

Each test drives real traffic through the gateway and checks what the worker, scoring and
policy engines produce and what the gateway then does with it.
"""
import asyncio
import os
import time

import httpx
import pytest

from tests.flow_harness import ENDPOINTS5, cid, running_system

THROTTLE_CAP = 5            # BUCKET_CAPACITY 20 x THROTTLED multiplier 0.25


@pytest.fixture
async def flow(monkeypatch):
    async with running_system(monkeypatch) as f:
        yield f


def mixed(n):
    return [ENDPOINTS5[i % 5] for i in range(n)]


async def warm_up(flow, key, seconds=40, rate=5):
    await flow.run(seconds, lambda i: {key: mixed(rate)})


async def assert_redis_consistent(flow, expect_all_requests_recorded=True):
    """Cross-check what is stored in Redis against itself and against what was sent."""
    r = flow.r
    await flow.app.state.telemetry.flush()               # requests sent after the last tick
    events = await flow.telemetry_events()
    reqs = [e for e in events if e.get("type") == "req"]
    if expect_all_requests_recorded:
        assert len(reqs) == flow.sent, "every request must have exactly one telemetry event"
        assert not [e for e in events if e.get("type") == "gap"]
    assert (await r.xpending("telemetry:events", "behaviour-workers"))["pending"] == 0

    # Risk log: stored score must equal the formula applied to the stored features.
    for _id, f in await r.xrange("risk:log"):
        if f["status"] in ("scored", "warming_up"):
            expected = 25 * (float(f["D"]) + float(f["B"]) + float(f["F"]) + float(f["C"]))
            assert float(f["score"]) == pytest.approx(expected, abs=0.02)
            assert 0.0 <= float(f["score"]) <= 100.0

    # Enforcement table matches per-client policy hashes.
    mult = {"MONITORED": "1", "THROTTLED": "0.25", "BLOCKED": "0"}
    enforced = await flow.enforced()
    for client, value in enforced.items():
        state, m, _updated = value.split(":")
        h = await r.hgetall(f"policy:{client}")
        assert h["state"] == state and m == mult[state] == h["multiplier"]
    for key in await r.keys("policy:*:*"):                  # per-client hashes: "policy:ip:..." / "policy:key:..."
        if key in ("policy:enforced", "policy:events"):
            continue
        h = await r.hgetall(key)
        client = key[len("policy:"):]
        assert (h["state"] != "NORMAL") == (client in enforced), f"{client}: table and hash disagree"

    # Transition log forms an unbroken chain per client, starting from NORMAL.
    chains: dict[str, str] = {}
    for _id, f in await r.xrange("policy:events"):
        assert f["from"] == chains.get(f["client"], "NORMAL"), f"broken chain for {f['client']}"
        assert f["from"] != f["to"]
        chains[f["client"]] = f["to"]
    for client, last in chains.items():
        assert (await r.hget(f"policy:{client}", "state")) == last


# =====================================================================================
# 1. Legitimate traffic
# =====================================================================================
async def test_legitimate_client_flows_end_to_end_and_stays_normal(flow):
    await warm_up(flow, "legit", seconds=60)
    codes = flow.statuses["legit"]
    assert set(codes) == {200} and len(codes) == 300

    h = await flow.policy("legit")
    assert h["status"] == "scored" and h["state"] == "NORMAL" and h["score"] == "0.00"
    assert float(h["baseline"]) == pytest.approx(5.0, abs=0.01)           # EWMA baseline learned
    assert (h["D"], h["B"], h["F"], h["C"]) == ("0.0000",) * 4
    assert await flow.enforced() == {}                                    # nothing restricted
    resp = await flow.send("legit", "/api/ping")
    assert "X-Policy-State" not in resp.headers and resp.headers["X-RateLimit-Limit"] == "20"
    await assert_redis_consistent(flow)


# =====================================================================================
# 2. Request burst -> THROTTLED -> recovery
# =====================================================================================
async def test_burst_is_throttled_then_access_is_restored_after_it_stops(flow):
    await warm_up(flow, "bursty")
    assert await flow.state("bursty") == "NORMAL"

    seen = []
    for i in range(25):                                    # 50 requests every second
        await flow.second({"bursty": mixed(50)})
        seen.append(await flow.state("bursty"))
    assert "THROTTLED" in seen and "BLOCKED" not in seen   # D and B saturate: 50 points
    h = await flow.policy("bursty")
    assert float(h["score"]) == pytest.approx(50.0) and (h["D"], h["B"]) == ("1.0000", "1.0000")
    assert "enter THROTTLED" in h["last_transition"]
    assert (await flow.enforced())[cid("bursty")].startswith("THROTTLED:0.25:")

    # The gateway now applies the smaller bucket: capacity 20 x 0.25.
    results = (await flow.second({"bursty": mixed(50)}))["bursty"]
    ok = [r for r in results if r.status_code == 200]
    assert len(ok) <= THROTTLE_CAP and all(r.headers["X-Policy-State"] == "THROTTLED" for r in results)
    assert results[0].headers["X-RateLimit-Limit"] == str(THROTTLE_CAP)

    # Recovery: the burst stops, the client behaves normally again.
    first_normal = None
    for i in range(150):
        await flow.second({"bursty": mixed(5)})
        if await flow.state("bursty") == "NORMAL":
            first_normal = i + 1
            break
    assert first_normal is not None, "client never recovered"
    assert first_normal <= 100
    steps = [to for _f, to in await flow.transitions("bursty")]
    assert steps[-3:] == ["THROTTLED", "MONITORED", "NORMAL"] or steps[-2:] == ["MONITORED", "NORMAL"]
    assert await flow.enforced() == {}
    resp = await flow.send("bursty", "/api/ping")
    assert resp.status_code == 200 and resp.headers["X-RateLimit-Limit"] == "20"
    assert flow.statuses["bursty"].count(500) == 0
    await assert_redis_consistent(flow)


# =====================================================================================
# 3. Repeated 4xx failures
# =====================================================================================
async def test_repeated_4xx_across_many_endpoints_raises_monitored_via_f_only(flow):
    probes = [f"/api/probe/p{i}" for i in range(10)]              # all 404 from the backend
    await flow.run(30, lambda i: {"scanner": probes})
    h = await flow.policy("scanner")
    assert h["state"] == "MONITORED" and float(h["score"]) == pytest.approx(25.0)
    assert (h["F"], h["C"]) == ("1.0000", "0.0000")
    assert set(flow.statuses["scanner"]) == {404}                 # passed through, not restricted
    resp = await flow.send("scanner", "/api/ping")
    assert resp.status_code == 200 and resp.headers["X-Policy-State"] == "MONITORED"
    assert resp.headers["X-RateLimit-Limit"] == "20"             # MONITORED does not shrink the bucket


async def test_credential_stuffing_one_failing_endpoint_is_throttled(flow):
    await flow.run(30, lambda i: {"stuffer": ["/api/fail/401"] * 10})
    h = await flow.policy("stuffer")
    assert h["state"] == "THROTTLED" and float(h["score"]) == pytest.approx(50.0)
    assert (h["F"], h["C"]) == ("1.0000", "1.0000")
    assert h["top_endpoint"] == "/api/fail/{id}"
    # A normal client is not affected.
    await warm_up(flow, "bystander2", seconds=5)
    assert await flow.state("bystander2") == "NORMAL"
    await assert_redis_consistent(flow)


async def test_gateway_429s_do_not_count_as_upstream_failures(flow):
    """A client flooding valid endpoints and being limited must not accumulate failure rate F."""
    await warm_up(flow, "flooder")
    await flow.run(20, lambda i: {"flooder": mixed(50)})
    h = await flow.policy("flooder")
    assert h["F"] == "0.0000" and int(h["n_4xx"]) == 0


# =====================================================================================
# 4. Concentrated endpoint access
# =====================================================================================
async def test_single_endpoint_at_normal_volume_is_monitored_not_restricted(flow):
    await flow.run(40, lambda i: {"poller": ["/api/search"] * 5})
    h = await flow.policy("poller")
    assert h["state"] == "MONITORED" and (h["C"], h["D"], h["B"], h["F"]) == ("1.0000", "0.0000", "0.0000", "0.0000")
    assert set(flow.statuses["poller"]) == {200}


async def test_flood_on_one_endpoint_is_blocked_stays_blocked_and_recovers_gradually(flow):
    await warm_up(flow, "attacker")
    for i in range(25):
        await flow.second({"attacker": ["/api/search"] * 60})
    h = await flow.policy("attacker")
    assert h["state"] == "BLOCKED" and float(h["score"]) == pytest.approx(75.0)
    assert (h["D"], h["B"], h["C"]) == ("1.0000", "1.0000", "1.0000")

    # BLOCKED: immediate 429 with Retry-After, flagged.
    res = (await flow.second({"attacker": ["/api/search"] * 60}))["attacker"]
    assert {r.status_code for r in res} == {429}
    assert res[0].headers["Retry-After"] == "60" and res[0].headers["X-Policy-State"] == "BLOCKED"
    assert "blocked" in res[0].json()["error"]
    # Continued abuse keeps the client blocked (blocked requests still count as demand).
    for i in range(30):
        await flow.second({"attacker": ["/api/search"] * 60})
    assert await flow.state("attacker") == "BLOCKED"

    # Abuse stops: BLOCKED -> THROTTLED -> MONITORED -> NORMAL, one level at a time.
    began, recovered = None, None
    for i in range(300):
        await flow.second({"attacker": []})
        if began is None and await flow.state("attacker") != "BLOCKED":
            began = i
        if await flow.state("attacker") == "NORMAL":
            recovered = i
            break
    assert recovered is not None
    steps = [to for _f, to in await flow.transitions("attacker")]
    assert steps[steps.index("BLOCKED"):] == ["BLOCKED", "THROTTLED", "MONITORED", "NORMAL"]
    assert began >= 8                             # score needs the 10 s span to clear before release
    # Dwell: the client had been BLOCKED for at least 60 s (event time) when it was released.
    log = [f for _i, f in await flow.r.xrange("policy:events") if f["client"] == cid("attacker")]
    entered = int(next(f["win"] for f in log if f["to"] == "BLOCKED"))
    left = int(next(f["win"] for f in log if f["from"] == "BLOCKED"))
    assert left - entered >= 60
    assert await flow.enforced() == {}
    resp = await flow.send("attacker", "/api/ping")
    assert resp.status_code == 200
    await assert_redis_consistent(flow)


# =====================================================================================
# 5. Concurrency
# =====================================================================================
async def test_concurrent_requests_never_exceed_the_bucket_through_the_whole_stack(flow):
    plan = {f"client{i}": ["/api/ping"] * 100 for i in range(3)}
    results = await flow.second(plan)
    for key, rs in results.items():
        assert sum(r.status_code == 200 for r in rs) == 20, key      # capacity 20, no refill (clock frozen)
        assert sum(r.status_code == 429 for r in rs) == 80
        assert rs[-1].headers.get("X-RateLimit-Remaining") in ("0", None)
    assert flow.app.state.telemetry.lost == 0
    await assert_redis_consistent(flow)                              # 303 requests == 303 events


async def test_concurrent_clients_do_not_interfere_with_each_others_policy(flow):
    await flow.run(40, lambda i: {"good": mixed(5), "evil": mixed(5)})   # both build a baseline
    for i in range(30):
        await flow.second({"good": mixed(5), "evil": ["/api/search"] * 60})
    assert await flow.state("evil") == "BLOCKED"
    assert await flow.state("good") == "NORMAL"
    assert set(flow.statuses["good"]) == {200}


async def test_attacker_without_history_is_capped_at_throttled(flow):
    """KNOWN LIMITATION (documented): with no baseline D is unavailable, so B + C = 50 at most."""
    for i in range(30):
        await flow.second({"newcomer": ["/api/search"] * 60})
    h = await flow.policy("newcomer")
    assert h["state"] == "THROTTLED" and float(h["score"]) == pytest.approx(50.0)
    assert h["unavailable"] == "D"


# =====================================================================================
# 6. Error handling
# =====================================================================================
@pytest.mark.skipif(bool(os.environ.get("REAL_REDIS_URL")),
                    reason="toggles the in-memory fake server's connectivity flag")
async def test_redis_outage_fails_open_reports_a_gap_and_never_changes_policy(flow):
    server_redis = flow.app.state.redis
    await warm_up(flow, "legit", seconds=30)
    before = await flow.policy("legit")
    sent_before = flow.sent

    # Take Redis down (shared fake server flag; for a real server this test is skipped below).
    server = server_redis.connection_pool.connection_kwargs["server"]
    server.connected = False
    outage = []
    for _ in range(5):
        res = await flow.second({"legit": mixed(5)}, worker=False)
        outage += res["legit"]
    assert all(r.status_code == 200 and r.headers["X-RateLimit-Status"] == "bypassed" for r in outage)
    lost_expected = flow.sent - sent_before
    server.connected = True

    await flow.run(25, lambda i: {"legit": mixed(5)})
    gaps = [e for e in await flow.telemetry_events() if e["type"] == "gap"]
    assert len(gaps) == 1 and int(gaps[0]["lost"]) == lost_expected     # loss is reported exactly
    h = await flow.policy("legit")
    assert h["state"] == "NORMAL" and h["status"] in ("scored", "warming_up")
    notes = [f["notes"] for _i, f in await flow.r.xrange("risk:log") if f["client"] == cid("legit")]
    assert float(before["baseline"]) == pytest.approx(float(h["baseline"]), abs=0.05)
    assert await flow.transitions("legit") == []                          # outage changed no state


async def test_backend_failure_returns_502_and_is_not_counted_as_client_failure(flow, monkeypatch):
    async def boom(*a, **k):
        raise httpx.ConnectError("backend down")
    monkeypatch.setattr(flow.app.state.http, "request", boom)
    await flow.run(20, lambda i: {"victim": mixed(10)})
    assert set(flow.statuses["victim"]) == {502}
    h = await flow.policy("victim")
    assert h["F"] == "0.0000" and int(h["n_4xx"]) == 0 and int(h["n_served"]) == 0
    events = [e for e in await flow.telemetry_events() if e["client"] == cid("victim")]
    assert {e["outcome"] for e in events} == {"bad_gateway"} and {e["up_status"] for e in events} == {"0"}


async def test_gateway_keeps_working_with_no_worker_running(flow):
    """Deterministic baseline protection does not depend on the behaviour worker."""
    await flow.run(10, lambda i: {"solo": mixed(30)}, worker=False)      # worker never runs
    codes = flow.statuses["solo"]
    assert codes.count(200) > 0 and codes.count(429) > 0 and set(codes) <= {200, 429}
    assert (await flow.r.xlen("telemetry:events")) == flow.sent          # telemetry still queued for later


async def test_malformed_telemetry_is_skipped_without_stopping_the_worker(flow):
    await flow.r.xadd("telemetry:events", {"type": "req", "client": "x"})
    await flow.r.xadd("telemetry:events", {"junk": "1"})
    await warm_up(flow, "legit", seconds=30)
    assert flow.worker.engine.bad_events == 2
    assert (await flow.policy("legit"))["state"] == "NORMAL"
    await assert_redis_consistent(flow, expect_all_requests_recorded=False)


# =====================================================================================
# 7. Asynchronous processing must not block the request path
# =====================================================================================
async def _time_requests(flow, n=100):
    t = time.perf_counter()
    for i in range(n):
        await flow.send("timing", "/api/ping")
    return time.perf_counter() - t


async def test_stalled_telemetry_publisher_does_not_slow_requests(flow):
    await _time_requests(flow, 20)                                       # warm
    normal = await _time_requests(flow)
    gate = asyncio.Event()
    original = flow.app.state.telemetry._publish

    async def stalled(batch):
        await gate.wait()
        await original(batch)
    flow.app.state.telemetry._publish = stalled
    stuck = await _time_requests(flow)
    assert stuck < normal * 3 + 0.25, (normal, stuck)
    gate.set()
    await flow.app.state.telemetry.flush()


async def test_stalled_policy_refresh_does_not_slow_requests(flow):
    await _time_requests(flow, 20)
    normal = await _time_requests(flow)

    async def never(*a, **k):
        await asyncio.sleep(30)
    flow.app.state.policy.refresh = never
    await asyncio.sleep(0.1)                                             # background loop now hangs inside refresh
    stuck = await _time_requests(flow)
    assert stuck < normal * 3 + 0.25, (normal, stuck)


async def test_telemetry_queue_overflow_never_fails_requests_and_is_reported(monkeypatch):
    async with running_system(monkeypatch, queue_size=5) as flow:
        tel = flow.app.state.telemetry
        gate = asyncio.Event()
        original = tel._publish

        async def stalled(batch):
            await gate.wait()
            await original(batch)
        tel._publish = stalled
        res = await flow.burst("overflow", ["/api/ping"] * 15)
        assert sum(r.status_code in (200, 429) for r in res) == 15       # every request answered
        assert tel.lost > 0
        gate.set()
        await tel.flush()
        await flow.send("overflow", "/api/ping")                         # next publish carries the gap report
        await tel.flush()
        events = await flow.telemetry_events()
        recorded = sum(e["type"] == "req" for e in events)
        reported = sum(int(e["lost"]) for e in events if e["type"] == "gap")
        assert recorded + reported == 16                                  # nothing lost silently


# =====================================================================================
# 8. Real clock smoke test: the asynchronous pipeline runs while requests are served
# =====================================================================================
async def test_real_time_pipeline_latency(monkeypatch):
    async with running_system(monkeypatch, simulated_clock=False) as f:
        # Windows close when the stream has moved `grace_s` (2 s) past them, so traffic must
        # span ~4 real seconds before the first window is evaluated.
        lag = []
        for _ in range(4):
            await f.burst("live", ["/api/ping"] * 5)
            t0 = time.perf_counter()
            await f.app.state.telemetry.flush()
            while await f.worker.process_once(block_ms=None):
                pass
            lag.append(time.perf_counter() - t0)       # publish + consume + score latency per batch
            await asyncio.sleep(1.0)
        h = await f.policy("live")
        assert h, "worker must have produced a policy record"
        assert h["status"] == "incomplete" and h["state"] == "NORMAL"     # cold start: no score yet
        assert "score" not in h
        assert max(lag) < 0.5, lag
