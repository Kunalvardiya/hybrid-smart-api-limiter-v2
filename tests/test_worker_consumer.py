"""Worker integration tests: Redis Stream in -> policy hash / risk log out (fakeredis)."""
import fakeredis
import pytest

from app.common.keys import CONSUMER_GROUP, RISK_LOG_STREAM, TELEMETRY_STREAM, policy_key
from app.worker.consumer import BehaviourWorker
from app.worker.params import ScoringParams

T0 = 2000


@pytest.fixture
def r():
    return fakeredis.FakeAsyncRedis(decode_responses=True)


async def emit(r, client, win, n, *, endpoint_cycle=("/a", "/b", "/c", "/d"), up_status=200):
    for i in range(n):
        await r.xadd(TELEMETRY_STREAM, {
            "v": 1, "type": "req", "client": client, "ts": win * 1000 + i, "win": win,
            "method": "GET", "endpoint": endpoint_cycle[i % len(endpoint_cycle)],
            "status": up_status, "up_status": up_status, "outcome": "allowed", "latency_ms": 1})


async def drain(w):
    while await w.process_once(block_ms=None):
        pass


async def test_scores_are_written_to_redis_and_events_acknowledged(r):
    w = BehaviourWorker(r, ScoringParams(warmup_evals=3))
    await w.setup()
    for win in range(T0, T0 + 30):
        await emit(r, "key:abc", win, 5)
    await drain(w)

    h = await r.hgetall(policy_key("key:abc"))
    assert h["status"] == "scored" and h["score"] == "0.00"
    assert {"D", "B", "F", "C", "baseline", "rate", "peak", "scored_win", "notes"} <= set(h)
    assert float(h["baseline"]) == pytest.approx(5.0)
    assert (await r.xpending(TELEMETRY_STREAM, CONSUMER_GROUP))["pending"] == 0
    assert await r.xlen(RISK_LOG_STREAM) >= 1


async def test_flood_is_scored_high_and_explained(r):
    w = BehaviourWorker(r, ScoringParams(warmup_evals=3))
    await w.setup()
    for win in range(T0, T0 + 40):
        await emit(r, "ip:9.9.9.9", win, 5)
    for win in range(T0 + 40, T0 + 55):
        await emit(r, "ip:9.9.9.9", win, 50)
    await drain(w)
    h = await r.hgetall(policy_key("ip:9.9.9.9"))
    assert float(h["score"]) == pytest.approx(50.0)
    assert h["D"] == "1.0000" and h["B"] == "1.0000" and h["F"] == "0.0000" and h["C"] == "0.0000"


async def test_cold_start_writes_incomplete_without_a_score(r):
    w = BehaviourWorker(r)
    await w.setup()
    for win in range(T0, T0 + 6):
        await emit(r, "ip:1.1.1.1", win, 5)
    await drain(w)
    h = await r.hgetall(policy_key("ip:1.1.1.1"))
    assert h["status"] == "incomplete" and "score" not in h
    assert await r.xlen(RISK_LOG_STREAM) == 0               # incomplete records are not logged as scores


async def test_gap_event_blocks_scoring_of_affected_windows(r):
    w = BehaviourWorker(r, ScoringParams(warmup_evals=3), batch=25)   # small batches: records are written as time passes
    await w.setup()
    for win in range(T0, T0 + 30):
        await emit(r, "ip:2.2.2.2", win, 5)
    await r.xadd(TELEMETRY_STREAM, {"v": 1, "type": "gap", "ts": 1, "lost": 20,
                                    "from_ts": (T0 + 30) * 1000, "to_ts": (T0 + 33) * 1000 + 999})
    for win in range(T0 + 34, T0 + 40):
        await emit(r, "ip:2.2.2.2", win, 5)
    await drain(w)
    h = await r.hgetall(policy_key("ip:2.2.2.2"))
    assert h["status"] == "incomplete" and "gap" in h["notes"]
    assert h["scored_win"] and int(h["scored_win"]) < T0 + 30    # last real score is visibly stale


async def test_unacked_events_are_recovered_after_restart(r):
    w1 = BehaviourWorker(r, ScoringParams(warmup_evals=3))
    await w1.setup()
    for win in range(T0, T0 + 20):
        await emit(r, "ip:3.3.3.3", win, 5)
    # Simulate a crash: events are delivered to the consumer but never acknowledged.
    await r.xreadgroup(CONSUMER_GROUP, "worker-1", {TELEMETRY_STREAM: ">"}, count=1000)
    assert (await r.xpending(TELEMETRY_STREAM, CONSUMER_GROUP))["pending"] == 100

    w2 = BehaviourWorker(r, ScoringParams(warmup_evals=3))
    await w2.setup()                                        # same consumer name => re-reads pending
    await drain(w2)
    assert (await r.xpending(TELEMETRY_STREAM, CONSUMER_GROUP))["pending"] == 0
    assert await r.exists(policy_key("ip:3.3.3.3"))


async def test_baseline_survives_a_worker_restart(r):
    w1 = BehaviourWorker(r, ScoringParams(warmup_evals=3))
    await w1.setup()
    for win in range(T0, T0 + 40):
        await emit(r, "ip:4.4.4.4", win, 5)
    await drain(w1)
    saved = float((await r.hgetall(policy_key("ip:4.4.4.4")))["baseline"])
    assert saved == pytest.approx(5.0)

    w2 = BehaviourWorker(r, ScoringParams(warmup_evals=3), consumer="worker-2")
    await w2.setup()
    for win in range(T0 + 40, T0 + 60):
        await emit(r, "ip:4.4.4.4", win, 5)
    await drain(w2)
    h = await r.hgetall(policy_key("ip:4.4.4.4"))
    assert float(h["baseline"]) == pytest.approx(5.0)       # restored, not reset to a fresh start
    assert int(h["obs_count"]) > 3


async def test_redis_write_failure_keeps_records_in_outbox(r, monkeypatch):
    w = BehaviourWorker(r, ScoringParams(warmup_evals=3))
    await w.setup()
    for win in range(T0, T0 + 30):
        await emit(r, "ip:5.5.5.5", win, 5)

    async def boom():
        raise OSError("redis down")
    real = w.flush_outbox
    monkeypatch.setattr(w, "flush_outbox", boom)
    with pytest.raises(OSError):
        await w.process_once(block_ms=None)
    assert w._outbox                                         # kept, not lost
    monkeypatch.setattr(w, "flush_outbox", real)
    await w.flush_outbox()
    assert await r.exists(policy_key("ip:5.5.5.5")) and not w._outbox
