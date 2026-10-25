"""End to end (fakeredis): telemetry stream -> scores -> states -> policy table -> recovery."""
import fakeredis
import pytest

from app.common.keys import TELEMETRY_STREAM, policy_key
from app.policy.params import PolicyParams
from app.policy.store import ENFORCED_HASH, POLICY_EVENTS_STREAM, parse_enforced
from app.worker.consumer import BehaviourWorker
from app.worker.params import ScoringParams

T0 = 5000
FOUR = ("/a", "/b", "/c", "/d")


@pytest.fixture
def r():
    return fakeredis.FakeAsyncRedis(decode_responses=True)


async def emit(r, client, win, n, endpoints=FOUR, up_status=200):
    for i in range(n):
        await r.xadd(TELEMETRY_STREAM, {
            "v": 1, "type": "req", "client": client, "ts": win * 1000 + i, "win": win,
            "method": "GET", "endpoint": endpoints[i % len(endpoints)], "status": up_status,
            "up_status": up_status, "outcome": "allowed", "latency_ms": 1})


async def drain(w):
    while await w.process_once(block_ms=None):
        pass


async def transitions(r, client):
    return [(f["from"], f["to"]) for _id, f in await r.xrange(POLICY_EVENTS_STREAM) if f["client"] == client]


def make_worker(r):
    return BehaviourWorker(r, ScoringParams(warmup_evals=3), PolicyParams(),
                           clock=lambda: 1_700_000_000.0)


async def run_seconds(r, w, start, seconds, abuser_rate, abuser_endpoints=FOUR, who="ip:6.6.6.6"):
    """One simulated second at a time, like live traffic. A quiet bystander keeps stream time moving."""
    for win in range(start, start + seconds):
        if abuser_rate:
            await emit(r, who, win, abuser_rate, abuser_endpoints)
        # rotates endpoints: a bystander hitting ONE endpoint would legitimately score C=1 (+25)
        await emit(r, "ip:bystander", win, 1, endpoints=(FOUR[win % 4],))
        await drain(w)
    return start + seconds


async def test_escalation_enforcement_and_gradual_recovery(r):
    w = make_worker(r)
    await w.setup()
    abuser = "ip:6.6.6.6"

    t = await run_seconds(r, w, T0, 40, 5)                       # normal behaviour
    assert (await r.hget(policy_key(abuser), "state")) == "NORMAL"
    assert not await r.hexists(ENFORCED_HASH, abuser)

    t = await run_seconds(r, w, t, 20, 50)                       # flood: D=1, B=1 -> score 50
    h = await r.hgetall(policy_key(abuser))
    assert h["state"] == "THROTTLED" and float(h["score"]) == pytest.approx(50.0)
    state, mult, _ = parse_enforced(await r.hget(ENFORCED_HASH, abuser))
    assert state.name == "THROTTLED" and mult == 0.25
    assert "enter THROTTLED" in h["last_transition"]

    t = await run_seconds(r, w, t, 20, 60, abuser_endpoints=("/only",))   # + one endpoint: 75
    h = await r.hgetall(policy_key(abuser))
    assert h["state"] == "BLOCKED" and float(h["score"]) == pytest.approx(75.0)
    assert h["multiplier"] == "0"
    assert parse_enforced(await r.hget(ENFORCED_HASH, abuser))[0].name == "BLOCKED"

    # The abuser goes completely silent. It must still be evaluated and step down gradually.
    t = await run_seconds(r, w, t, 40, 0)
    assert (await r.hget(policy_key(abuser), "state")) == "BLOCKED"       # still inside its 60 s dwell
    t = await run_seconds(r, w, t, 100, 0)

    h = await r.hgetall(policy_key(abuser))
    assert h["state"] == "NORMAL"
    assert not await r.hexists(ENFORCED_HASH, abuser)                      # access fully restored

    steps = [to for _frm, to in await transitions(r, abuser)]
    tail = steps[steps.index("BLOCKED"):]
    assert tail == ["BLOCKED", "THROTTLED", "MONITORED", "NORMAL"]          # one level at a time
    reasons = [f["reason"] for _id, f in await r.xrange(POLICY_EVENTS_STREAM) if f["client"] == abuser]
    assert any("dwell" in x for x in reasons)
    # the bystander never left NORMAL
    assert await transitions(r, "ip:bystander") == []


async def test_restricted_client_with_no_baseline_is_released_after_it_goes_silent(r):
    """Regression (found on the Docker stack): scanners never learn a baseline (their score is always
    >= 20), so after they stopped they used to stay MONITORED forever."""
    w = make_worker(r)
    await w.setup()
    scanner = "ip:7.7.7.7"
    probes = tuple(f"/p{i}" for i in range(10))
    for win in range(T0, T0 + 40):
        await emit(r, scanner, win, 10, probes, up_status=404)
        await emit(r, "ip:bystander", win, 1, endpoints=(FOUR[win % 4],))
        await drain(w)
    assert (await r.hget(policy_key(scanner), "state")) == "MONITORED"
    await run_seconds(r, w, T0 + 40, 60, 0)
    assert (await r.hget(policy_key(scanner), "state")) == "NORMAL"
    assert not await r.hexists(ENFORCED_HASH, scanner)


async def test_blocked_client_recovers_when_the_whole_system_goes_quiet_thanks_to_ticks(r):
    """The attack stops and NOBODY else sends traffic: only the gateway heartbeat moves time forward."""
    w = make_worker(r)
    await w.setup()
    abuser = "ip:6.6.6.6"
    t = await run_seconds(r, w, T0, 40, 5)
    t = await run_seconds(r, w, t, 25, 60, abuser_endpoints=("/only",))
    assert (await r.hget(policy_key(abuser), "state")) == "BLOCKED"

    async def tick(win):
        await r.xadd(TELEMETRY_STREAM, {"v": 1, "type": "tick", "ts": win * 1000, "win": win})
        await drain(w)

    for win in range(t, t + 25):                   # a bystander is still sending for a short while
        await emit(r, "ip:bystander", win, 1, endpoints=(FOUR[win % 4],))
        await drain(w)
    for win in range(t + 25, t + 260):             # then total silence: ticks only
        await tick(win)
    assert (await r.hget(policy_key(abuser), "state")) == "NORMAL"
    assert not await r.hexists(ENFORCED_HASH, abuser)
    steps = [to for _f, to in await transitions(r, abuser)]
    assert steps[steps.index("BLOCKED"):] == ["BLOCKED", "THROTTLED", "MONITORED", "NORMAL"]


async def test_without_ticks_a_silent_system_does_not_pretend_the_client_recovered(r):
    """Safety: no heartbeat and no traffic means the telemetry path may be down, so nothing is released."""
    w = make_worker(r)
    await w.setup()
    abuser = "ip:6.6.6.6"
    t = await run_seconds(r, w, T0, 40, 5)
    t = await run_seconds(r, w, t, 25, 60, abuser_endpoints=("/only",))
    assert (await r.hget(policy_key(abuser), "state")) == "BLOCKED"
    for _ in range(50):
        await drain(w)                              # time passes, nothing arrives
    assert (await r.hget(policy_key(abuser), "state")) == "BLOCKED"


async def test_enforcement_is_restored_after_a_worker_restart(r):
    w1 = make_worker(r)
    await w1.setup()
    t = await run_seconds(r, w1, T0, 40, 5)
    t = await run_seconds(r, w1, t, 25, 60, abuser_endpoints=("/only",))
    assert (await r.hget(policy_key("ip:6.6.6.6"), "state")) == "BLOCKED"

    w2 = BehaviourWorker(r, ScoringParams(warmup_evals=3), PolicyParams(), consumer="worker-2",
                         clock=lambda: 1_700_000_000.0)
    await w2.setup()
    assert w2.policy.state_of("ip:6.6.6.6").name == "BLOCKED"              # not reset to NORMAL
    assert "ip:6.6.6.6" in w2.policy.pinned()


async def test_incomplete_telemetry_never_changes_state(r):
    w = make_worker(r)
    await w.setup()
    t = await run_seconds(r, w, T0, 40, 5)
    t = await run_seconds(r, w, t, 20, 50)
    assert (await r.hget(policy_key("ip:6.6.6.6"), "state")) == "THROTTLED"
    await r.xadd(TELEMETRY_STREAM, {"v": 1, "type": "gap", "ts": 1, "lost": 500,
                                    "from_ts": t * 1000, "to_ts": (t + 20) * 1000 + 999})
    t2 = await run_seconds(r, w, t + 21, 6, 0)                              # silence while spans overlap the gap
    h = await r.hgetall(policy_key("ip:6.6.6.6"))
    assert h["status"] == "incomplete" and h["state"] == "THROTTLED"       # not released by missing data
    await run_seconds(r, w, t2, 10, 0)                                      # gap has left the span: 10 s of
    h = await r.hgetall(policy_key("ip:6.6.6.6"))                           # genuinely observed silence
    assert h["state"] == "MONITORED"                                        # now recovery may proceed
