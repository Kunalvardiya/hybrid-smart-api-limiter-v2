"""Read-only monitoring API and dashboard page, served by the gateway.

Everything here is DERIVED from data the system already keeps in Redis; nothing is invented:

  telemetry:events   last N seconds of request events  -> totals, outcomes, rates, bursts, failures
  policy:{client}    latest score record per client     -> risk score, D/B/F/C, state, rate, peak
  policy:enforced    clients currently restricted       -> state counts
  policy:events      state-change log                   -> "policy updates"
  worker:heartbeat   written by the worker every ~1 s   -> worker health
  gateway objects    telemetry publisher / policy cache counters

Limits worth knowing (also shown on the dashboard):
  * Request totals are for a recent WINDOW read from the capped telemetry stream, not lifetime totals;
    no lifetime counters are kept because that would add Redis writes to the request path.
  * If the window holds more than `cap` events it is truncated to the newest `cap` and says so.

Security: disabled unless ADMIN_TOKEN is set; every endpoint then requires it in the X-Admin-Token
header (constant-time comparison). Client ids are SHA-256-derived API-key labels or "ip:<address>"
(personal data): keep this on localhost. Raw API keys are never stored, so they cannot be shown.
"""
import hmac
import time
from collections import Counter, defaultdict
from pathlib import Path

import redis.asyncio as redis_lib
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from app.common.keys import CONSUMER_GROUP, TELEMETRY_STREAM
from app.policy.store import ENFORCED_HASH, POLICY_EVENTS_STREAM

HEARTBEAT_KEY = "worker:heartbeat"
STATE_ORDER = {"NORMAL": 0, "MONITORED": 1, "THROTTLED": 2, "BLOCKED": 3}
OUTCOMES = ("allowed", "limited", "blocked", "bypassed", "rejected", "bad_gateway")
STATIC = Path(__file__).parent / "static"

router = APIRouter()


def require_admin(request: Request) -> None:
    token = request.app.state.settings.admin_token
    if not token:
        raise HTTPException(503, "admin API disabled: set ADMIN_TOKEN to enable it")
    supplied = request.headers.get("x-admin-token", "")
    if not hmac.compare_digest(supplied.encode(), token.encode()):
        raise HTTPException(401, "missing or wrong X-Admin-Token")


def _num(value: str | None, cast=float):
    if value in (None, ""):
        return None
    try:
        return cast(value)
    except ValueError:
        return None


# ---- window statistics from the telemetry stream ----------------------------------------------
async def window_stats(r: redis_lib.Redis, seconds: int, cap: int) -> dict:
    server_s, server_us = await r.time()
    now_ms = int(server_s) * 1000 + int(server_us) // 1000
    entries = await r.xrevrange(TELEMETRY_STREAM, max="+", min=str(now_ms - seconds * 1000), count=cap + 1)
    truncated = len(entries) > cap
    entries = entries[:cap]

    outcomes: Counter = Counter()
    per_win: dict[int, Counter] = defaultdict(Counter)
    served = failures = gaps = lost = 0
    clients = set()
    for _id, f in entries:
        if f.get("type") == "gap":
            gaps += 1
            lost += _num(f.get("lost"), int) or 0
            continue
        if f.get("type") != "req":
            continue
        outcome = f.get("outcome", "unknown")
        up = _num(f.get("up_status"), int) or 0
        win = _num(f.get("win"), int)
        outcomes[outcome] += 1
        clients.add(f.get("client"))
        restricted = outcome in ("limited", "blocked")
        failed = 400 <= up < 500
        if up > 0:
            served += 1
            failures += failed
        if win is not None:
            b = per_win[win]
            b["total"] += 1
            b["restricted"] += restricted
            b["failures"] += failed
    total = sum(outcomes.values())
    wins = sorted(per_win)
    covered = max(1, (wins[-1] - wins[0] + 1)) if wins else seconds
    return {
        "seconds": seconds, "events_scanned": len(entries), "truncated": truncated, "cap": cap,
        "total": total,
        "outcomes": {k: outcomes.get(k, 0) for k in OUTCOMES} | {k: v for k, v in outcomes.items() if k not in OUTCOMES},
        "restricted": outcomes["limited"] + outcomes["blocked"],
        "active_clients": len(clients),
        "upstream": {"served": served, "failures_4xx": failures,
                     "failure_rate": (failures / served) if served else None},
        "avg_rps": round(total / covered, 2) if total else 0.0,
        "peak_per_second": max((per_win[w]["total"] for w in wins), default=0),
        "gap_events": gaps, "events_lost_reported": lost,
        "series": [{"win": w, **{k: per_win[w][k] for k in ("total", "restricted", "failures")}}
                   for w in wins[-seconds:]],
    }


async def worker_health(r: redis_lib.Redis) -> dict:
    hb = await r.hgetall(HEARTBEAT_KEY)
    if not hb:
        return {"status": "unknown", "detail": "no heartbeat: the worker is not running, or has not started yet"}
    age = max(0.0, time.time() - (_num(hb.get("ts_ms")) or 0) / 1000)
    return {"status": "ok" if age < 5 else "stale", "age_s": round(age, 1),
            "processed": _num(hb.get("processed"), int), "clients_tracked": _num(hb.get("clients_tracked"), int),
            "closed_win": _num(hb.get("closed_win"), int), "late_events": _num(hb.get("late_events"), int),
            "bad_events": _num(hb.get("bad_events"), int), "outbox": _num(hb.get("outbox"), int)}


async def stream_health(r: redis_lib.Redis) -> dict:
    out = {"length": await r.xlen(TELEMETRY_STREAM), "pending": None, "lag": None}
    try:
        for g in await r.xinfo_groups(TELEMETRY_STREAM):
            if g.get("name") == CONSUMER_GROUP:
                out["pending"] = g.get("pending")
                out["lag"] = g.get("lag")
    except redis_lib.ResponseError:
        pass                                           # stream or group does not exist yet
    return out


def _redis_down(exc) -> JSONResponse:
    return JSONResponse({"error": "redis unavailable", "detail": str(exc), "redis": {"ok": False}},
                        status_code=503)


@router.get("/admin/overview", dependencies=[Depends(require_admin)])
async def overview(request: Request, seconds: int = Query(60, ge=5, le=600),
                   cap: int = Query(5_000, ge=100, le=100_000)):
    app = request.app
    r = app.state.admin_redis
    try:
        window = await window_stats(r, seconds, cap)
        worker = await worker_health(r)
        stream = await stream_health(r)
        enforced = Counter(v.split(":")[0] for v in (await r.hgetall(ENFORCED_HASH)).values())
    except (redis_lib.RedisError, OSError) as exc:
        return _redis_down(exc)
    return {
        "generated_ms": int(time.time() * 1000),
        "health": {"redis": {"ok": True}, "worker": worker, "stream": stream,
                   "gateway": {"telemetry": app.state.telemetry.stats(), "policy_cache": app.state.policy.stats()}},
        "window": window,
        "enforced": {s: enforced.get(s, 0) for s in ("MONITORED", "THROTTLED", "BLOCKED")},
    }


def _client_row(client: str, h: dict) -> dict:
    return {
        "client": client, "state": h.get("state", "NORMAL"), "status": h.get("status"),
        "score": _num(h.get("score")), "D": _num(h.get("D")), "B": _num(h.get("B")),
        "F": _num(h.get("F")), "C": _num(h.get("C")),
        "rate": _num(h.get("rate")), "peak": _num(h.get("peak"), int), "baseline": _num(h.get("baseline")),
        "n_requests": _num(h.get("n_requests"), int), "n_served": _num(h.get("n_served"), int),
        "n_4xx": _num(h.get("n_4xx"), int), "top_endpoint": h.get("top_endpoint") or None,
        "unavailable": [x for x in (h.get("unavailable") or "").split(",") if x],
        "state_since_win": _num(h.get("state_since"), int), "last_transition": h.get("last_transition") or None,
        "notes": h.get("notes") or None,
    }


@router.get("/admin/clients", dependencies=[Depends(require_admin)])
async def clients(request: Request, limit: int = Query(50, ge=1, le=200)):
    r = request.app.state.admin_redis
    try:
        keys: list[str] = []
        for pattern in ("policy:key:*", "policy:ip:*"):
            keys += [k async for k in r.scan_iter(match=pattern, count=500)]
        rows = []
        async with r.pipeline(transaction=False) as pipe:
            for k in keys:
                pipe.hgetall(k)
            hashes = await pipe.execute()
    except (redis_lib.RedisError, OSError) as exc:
        return _redis_down(exc)
    rows = [_client_row(k[len("policy:"):], h) for k, h in zip(keys, hashes) if h]
    rows.sort(key=lambda x: (-STATE_ORDER.get(x["state"], 0), -(x["score"] or 0.0), x["client"]))
    return {"total_clients": len(rows), "returned": min(limit, len(rows)), "clients": rows[:limit]}


@router.get("/admin/events", dependencies=[Depends(require_admin)])
async def policy_events(request: Request, limit: int = Query(50, ge=1, le=200)):
    r = request.app.state.admin_redis
    try:
        entries = await r.xrevrange(POLICY_EVENTS_STREAM, count=limit)
    except (redis_lib.RedisError, OSError) as exc:
        return _redis_down(exc)
    return {"events": [{"id": i, "client": f.get("client"), "from": f.get("from"), "to": f.get("to"),
                        "win": _num(f.get("win"), int), "score": _num(f.get("score")),
                        "D": _num(f.get("D")), "B": _num(f.get("B")), "F": _num(f.get("F")),
                        "C": _num(f.get("C")), "reason": f.get("reason")} for i, f in entries]}


@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard():
    """The page itself carries no data; its requests to /admin/* need the token."""
    return HTMLResponse((STATIC / "dashboard.html").read_text(encoding="utf-8"),
                        headers={"Cache-Control": "no-store"})
