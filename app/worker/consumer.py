"""Redis Streams consumer: feeds telemetry to the engines and stores the results.

Delivery model: events are acknowledged as soon as they are inside the engine's
memory (at-most-once from then on). Records waiting to be written live in an outbox
and are retried if Redis hiccups. If the worker crashes, in-memory windows are lost;
after a restart the engine reports "incomplete" until it has observed a full span again
instead of scoring partial data, and the enforcement state is restored from Redis.

Redis output
  policy:{client}   hash    latest record: score, D, B, F, C, baseline, status, notes,
                            plus state, state_since, multiplier, state_reason
  policy:enforced   hash    client -> "STATE:multiplier:updated_ms" for non-NORMAL clients
                            (this is what the gateway reads)
  policy:events     stream  one entry per state change, with the reason
  risk:log          stream  capped history of score records for clients with traffic
  worker:heartbeat  hash    liveness + counters for the dashboard (expires after 30 s)
"""
import asyncio
import logging
import time
from collections.abc import Callable

import redis.asyncio as redis

from app.common.keys import CONSUMER_GROUP, RISK_LOG_STREAM, TELEMETRY_STREAM, policy_key
from app.policy.engine import PolicyEngine
from app.policy.machine import Decision
from app.policy.params import PolicyParams, State
from app.policy.store import ENFORCED_HASH, POLICY_EVENTS_STREAM, format_enforced
from app.worker.engine import BehaviourEngine, ScoreRecord
from app.worker.params import ScoringParams

log = logging.getLogger("worker")
HEARTBEAT_KEY = "worker:heartbeat"


class BehaviourWorker:
    def __init__(self, client: redis.Redis, params: ScoringParams | None = None,
                 policy_params: PolicyParams | None = None, *,
                 stream: str = TELEMETRY_STREAM, group: str = CONSUMER_GROUP,
                 consumer: str = "worker-1", batch: int = 500, log_maxlen: int = 50_000,
                 clock: Callable[[], float] = time.time):
        self.r = client
        self.engine = BehaviourEngine(params)
        self.policy = PolicyEngine(policy_params)
        self.engine.keep_alive = self.policy.pinned
        self.stream, self.group, self.consumer = stream, group, consumer
        self.batch, self.log_maxlen, self.clock = batch, log_maxlen, clock
        self._outbox: dict[str, tuple[ScoreRecord, Decision]] = {}
        self._events_outbox: list[tuple[ScoreRecord, Decision]] = []
        self._stop = asyncio.Event()
        self.processed = 0                 # telemetry events consumed since this worker started
        self._last_beat = 0.0

    async def setup(self) -> None:
        try:
            await self.r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        await self._restore_enforced()
        # Re-read anything this consumer received but never acknowledged.
        while await self._read("0", block=None):
            pass

    async def _restore_enforced(self) -> None:
        """After a restart, resume enforcement for clients that were not NORMAL."""
        for client in await self.r.hkeys(ENFORCED_HASH):
            state_s, since, baseline, obs = await self.r.hmget(
                policy_key(client), "state", "state_since", "baseline", "obs_count")
            try:
                self.policy.restore(client, State[state_s], int(since))
                self.engine.seed_client(client, float(baseline) if baseline else None, int(obs or 0))
            except (KeyError, TypeError, ValueError):
                log.warning("could not restore policy for %s; leaving it NORMAL", client)

    async def _read(self, start_id: str, block: int | None) -> int:
        resp = await self.r.xreadgroup(self.group, self.consumer, {self.stream: start_id},
                                       count=self.batch, block=block)
        if not resp:
            return 0
        entries = resp[0][1]
        if not entries:
            return 0
        await self._handle(entries)
        return len(entries)

    async def _handle(self, entries: list) -> None:
        events = [fields for _id, fields in entries]
        wins = [int(e["win"]) for e in events
                if e.get("type") in ("req", "tick") and str(e.get("win", "")).isdigit()]
        if wins:
            self.engine.begin_observation(min(wins))
        await self._seed_new_clients(events)
        for e in events:
            self.engine.ingest(e)
        await self.r.xack(self.stream, self.group, *[_id for _id, _ in entries])
        for rec in self.engine.advance():
            decision = self.policy.apply(rec)
            self._outbox[rec.client] = (rec, decision)
            if decision.changed:
                self._events_outbox.append((rec, decision))
                log.info("policy %s: %s -> %s (%s)", rec.client, decision.previous.name,
                         decision.state.name, decision.reason)

    async def _seed_new_clients(self, events: list[dict]) -> None:
        new = {e["client"] for e in events
               if e.get("type") == "req" and "client" in e and e["client"] not in self.engine.clients}
        if not new:
            return
        async with self.r.pipeline(transaction=False) as pipe:
            for c in new:
                pipe.hmget(policy_key(c), "baseline", "obs_count")
            rows = await pipe.execute()
        for c, (baseline, obs) in zip(new, rows):
            try:
                self.engine.seed_client(c, float(baseline) if baseline else None, int(obs or 0))
            except ValueError:
                pass

    async def flush_outbox(self) -> None:
        if not self._outbox and not self._events_outbox:
            return
        items = list(self._outbox.items())
        events = list(self._events_outbox)
        now_ms = int(self.clock() * 1000)
        async with self.r.pipeline(transaction=False) as pipe:
            for client, (rec, d) in items:
                fields = rec.to_fields()
                fields.update({"state": d.state.name, "state_since": str(d.since_win),
                               "multiplier": f"{self.policy.p.multiplier(d.state):g}",
                               "state_reason": d.reason})
                if d.changed:    # kept until the next change, unlike state_reason (latest evaluation)
                    fields["last_transition"] = f"{d.previous.name}->{d.state.name}: {d.reason}"
                pipe.hset(policy_key(client), mapping=fields)
                if d.state == State.NORMAL:
                    pipe.hdel(ENFORCED_HASH, client)
                else:
                    pipe.hset(ENFORCED_HASH, client,
                              format_enforced(d.state, self.policy.p.multiplier(d.state), now_ms))
                if rec.n_requests > 0 and rec.status != "incomplete":
                    pipe.xadd(RISK_LOG_STREAM, {"client": client, **rec.to_fields()},
                              maxlen=self.log_maxlen, approximate=True)
            for rec, d in events:
                pipe.xadd(POLICY_EVENTS_STREAM, {
                    "client": rec.client, "from": d.previous.name, "to": d.state.name, "win": d.since_win,
                    "score": "" if rec.score is None else f"{rec.score:.2f}",
                    "D": rec.D if rec.D is not None else "", "B": rec.B if rec.B is not None else "",
                    "F": rec.F if rec.F is not None else "", "C": rec.C if rec.C is not None else "",
                    "reason": d.reason}, maxlen=self.log_maxlen, approximate=True)
            await pipe.execute()
        for client, item in items:   # drop only what was written (newer records may have replaced them)
            if self._outbox.get(client) is item:
                del self._outbox[client]
        del self._events_outbox[:len(events)]

    async def heartbeat(self) -> None:
        """Tell dashboards the worker is alive. Best effort, at most once a second; expires after 30 s."""
        now = self.clock()
        if now - self._last_beat < 1.0:
            return
        self._last_beat = now
        e = self.engine
        try:
            async with self.r.pipeline(transaction=False) as pipe:
                pipe.hset(HEARTBEAT_KEY, mapping={
                    "ts_ms": int(now * 1000), "processed": self.processed,
                    "clients_tracked": len(e.clients), "closed_win": "" if e.closed is None else e.closed,
                    "late_events": e.late_events, "bad_events": e.bad_events,
                    "outbox": len(self._outbox)})
                pipe.pexpire(HEARTBEAT_KEY, 30_000)
                await pipe.execute()
        except (redis.RedisError, OSError):
            pass                                   # health reporting must never break processing

    async def process_once(self, block_ms: int | None = 1000) -> int:
        n = await self._read(">", block_ms)
        self.processed += n
        await self.flush_outbox()
        await self.heartbeat()
        return n

    async def run(self) -> None:
        while True:
            try:
                await self.setup()
                break
            except (redis.RedisError, OSError) as exc:
                log.warning("worker setup failed: %s", exc)
                await asyncio.sleep(1)
        log.info("behaviour worker started")
        while not self._stop.is_set():
            try:
                await self.process_once()
            except (redis.RedisError, OSError) as exc:
                log.warning("redis error in worker loop (will retry): %s", exc)
                await asyncio.sleep(1)

    def stop(self) -> None:
        self._stop.set()
