"""Request telemetry: one small event per /api request, published to a Redis Stream.

Off the critical path: the request handler only calls `record()`, which puts the
event on an in-memory queue (no I/O, never blocks, never raises). A background
task drains the queue and writes batches to Redis with XADD.

Honesty about loss: if an event is dropped (queue full) or a batch cannot be
written (Redis down), the publisher counts it and, as soon as Redis is reachable
again, writes a "gap" event describing how many events were lost and over which
time span. The behaviour worker must treat windows overlapping a gap as
incomplete instead of scoring them as if they were quiet. See docs/TELEMETRY.md.
"""
import asyncio
import logging
import re
import time

import redis.asyncio as redis

from app.common.keys import TELEMETRY_STREAM

log = logging.getLogger("telemetry")

SCHEMA_VERSION = 1

_NUMERIC = re.compile(r"^\d+$")
_UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_LONG_HEX = re.compile(r"^[0-9a-fA-F]{16,}$")


def normalize_endpoint(path: str) -> str:
    """Collapse IDs so /api/items/1 and /api/items/2 count as one endpoint.

    Without this, a scraper walking /items/1../items/9999 would look like it was
    spread across thousands of endpoints and hide its low endpoint concentration.
    The query string is never included (it can carry tokens).
    """
    segments = []
    for seg in path.split("?", 1)[0].split("/"):
        if _NUMERIC.match(seg) or _UUID.match(seg) or _LONG_HEX.match(seg):
            seg = "{id}"
        segments.append(seg[:64])
    return ("/".join(segments) or "/")[:200]


def build_event(*, client: str, method: str, path: str, ts_ms: int, status: int,
                up_status: int, outcome: str, latency_ms: int) -> dict:
    """The only fields we ever record. No headers, bodies, query strings or raw keys."""
    return {
        "v": SCHEMA_VERSION,
        "type": "req",
        "client": client,            # "key:<hash>" or "ip:<addr>", never a raw API key
        "ts": ts_ms,                 # gateway wall-clock ms when the request arrived
        "win": ts_ms // 1000,        # one-second measurement window id
        "method": method,
        "endpoint": normalize_endpoint(path),
        "status": status,            # status returned to the client
        "up_status": up_status,      # status from the protected API, 0 if it never answered
        "outcome": outcome,          # allowed | limited | bypassed | rejected | bad_gateway
        "latency_ms": latency_ms,
    }


class TelemetryPublisher:
    def __init__(self, client: redis.Redis, stream: str = TELEMETRY_STREAM,
                 maxlen: int = 100_000, queue_size: int = 10_000, batch_size: int = 100,
                 tick_s: float = 0.0):
        self._client = client
        self._tick_s = tick_s
        self._stream = stream
        self._maxlen = maxlen
        self._batch_size = batch_size
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self._task: asyncio.Task | None = None
        self._tick_task: asyncio.Task | None = None
        self.published = 0
        self.lost = 0                       # lifetime total of lost events
        self._gap_lost = 0                  # lost events not yet reported to the stream
        self._gap_from: int | None = None
        self._gap_to: int | None = None

    # ---- lifecycle -------------------------------------------------------
    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="telemetry-publisher")
        if self._tick_s > 0:
            self._tick_task = asyncio.create_task(self._ticker(), name="telemetry-ticker")

    async def flush(self) -> None:
        """Wait until everything queued so far has been attempted."""
        await self._queue.join()

    async def stop(self, timeout: float = 2.0) -> None:
        try:
            await asyncio.wait_for(self.flush(), timeout)
        except asyncio.TimeoutError:
            log.warning("telemetry shutdown: %d events not flushed", self._queue.qsize())
        for task in (self._tick_task, self._task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    # ---- hot path --------------------------------------------------------
    def record(self, event: dict) -> bool:
        """Non-blocking and never raises. Returns False if the event was dropped."""
        try:
            self._queue.put_nowait(event)
            return True
        except asyncio.QueueFull:
            self._note_loss(1, event["ts"])
            return False

    def stats(self) -> dict:
        return {"queued": self._queue.qsize(), "published": self.published,
                "lost": self.lost, "unreported_gap": self._gap_lost}

    # ---- background ------------------------------------------------------
    def _note_loss(self, n: int, ts: int) -> None:
        self.lost += n
        self._gap_lost += n
        self._gap_from = ts if self._gap_from is None else min(self._gap_from, ts)
        self._gap_to = ts if self._gap_to is None else max(self._gap_to, ts)

    async def _ticker(self) -> None:
        """Heartbeat events: "this gateway is alive and its telemetry path works".

        They let the worker move time forward while there is no traffic, so a client that has stopped
        can be seen to be quiet and recover. If the gateway or Redis is down, ticks stop and the worker
        correctly does NOT treat the silence as quiet. Ticks are never counted as lost events.
        """
        while True:
            await asyncio.sleep(self._tick_s)
            ms = int(time.time() * 1000)
            try:
                self._queue.put_nowait({"v": SCHEMA_VERSION, "type": "tick", "ts": ms, "win": ms // 1000})
            except asyncio.QueueFull:
                pass

    async def _run(self) -> None:
        while True:
            batch = [await self._queue.get()]
            while len(batch) < self._batch_size:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            try:
                await self._publish(batch)
            except Exception as exc:  # never let the publisher task die
                log.warning("telemetry publish failed, %d events lost: %s", len(batch), exc)
                for e in batch:
                    if e.get("type") == "req":          # a lost tick is not data loss
                        self._note_loss(1, e["ts"])
            finally:
                for _ in batch:
                    self._queue.task_done()

    async def _publish(self, batch: list[dict]) -> None:
        gap = None
        if self._gap_lost:
            gap = {"v": SCHEMA_VERSION, "type": "gap", "ts": int(time.time() * 1000),
                   "lost": self._gap_lost, "from_ts": self._gap_from, "to_ts": self._gap_to}
        async with self._client.pipeline(transaction=False) as pipe:
            if gap:
                pipe.xadd(self._stream, gap, maxlen=self._maxlen, approximate=True)
            for event in batch:
                pipe.xadd(self._stream, event, maxlen=self._maxlen, approximate=True)
            await pipe.execute()
        self.published += sum(1 for e in batch if e.get("type") == "req")
        if gap:   # reported; keep anything lost while the write was in flight
            self._gap_lost -= gap["lost"]
            if self._gap_lost == 0:
                self._gap_from = self._gap_to = None
