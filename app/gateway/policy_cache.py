"""The gateway's local copy of the enforcement table.

Fast path: `get()` is a plain dictionary lookup. It does no I/O and is not a coroutine,
so reading policy can never slow a request.
Slow path: a background task copies Redis hash `policy:enforced` into the dictionary every
`refresh_s` seconds and swaps it in with one assignment.

Staleness rules (fail toward the plain token bucket, which is the deterministic baseline):
  * an entry whose `updated_ms` is older than `max_age_s` is ignored: the behaviour worker
    refreshes entries on every evaluation, so an old entry means the worker has stopped;
  * if the table cannot be refreshed for `max_age_s`, the whole cache is ignored.
Between refreshes a client can be up to `refresh_s` behind the latest policy.
"""
import asyncio
import logging
import time
from dataclasses import dataclass

import redis.asyncio as redis

from app.policy.params import State
from app.policy.store import ENFORCED_HASH, parse_enforced

log = logging.getLogger("policy-cache")


@dataclass(frozen=True)
class PolicyView:
    state: State
    multiplier: float


NORMAL_VIEW = PolicyView(State.NORMAL, 1.0)


class PolicyCache:
    def __init__(self, client: redis.Redis, refresh_s: float = 1.0, max_age_s: float = 120.0):
        self._client = client
        self._refresh_s = refresh_s
        self._max_age_s = max_age_s
        self._view: dict[str, PolicyView] = {}
        self._last_ok: float | None = None       # monotonic time of last successful refresh
        self._task: asyncio.Task | None = None
        self.refresh_errors = 0
        self.ignored_stale = 0

    def get(self, client_id: str) -> PolicyView:
        """O(1), no I/O."""
        if self._last_ok is None or time.monotonic() - self._last_ok > self._max_age_s:
            return NORMAL_VIEW
        return self._view.get(client_id, NORMAL_VIEW)

    async def refresh(self) -> None:
        raw = await self._client.hgetall(ENFORCED_HASH)
        now_ms = int(time.time() * 1000)
        view: dict[str, PolicyView] = {}
        stale = 0
        for client_id, value in raw.items():
            parsed = parse_enforced(value)
            if parsed is None:
                log.warning("ignoring malformed policy entry for %s", client_id)
                continue
            state, multiplier, updated_ms = parsed
            if now_ms - updated_ms > self._max_age_s * 1000:
                stale += 1
                continue
            view[client_id] = PolicyView(state, multiplier)
        self._view = view                       # single atomic swap
        self.ignored_stale = stale
        self._last_ok = time.monotonic()

    async def _run(self) -> None:
        while True:
            try:
                await self.refresh()
            except (redis.RedisError, OSError) as exc:
                self.refresh_errors += 1
                log.warning("policy refresh failed (keeping last known policy for up to %ss): %s",
                            self._max_age_s, exc)
            await asyncio.sleep(self._refresh_s)

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="policy-cache")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def stats(self) -> dict:
        age = None if self._last_ok is None else round(time.monotonic() - self._last_ok, 2)
        return {"entries": len(self._view), "age_s": age, "refresh_errors": self.refresh_errors,
                "ignored_stale": self.ignored_stale}
