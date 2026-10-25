"""API gateway: identifies the client, applies the token bucket, forwards, records telemetry.

Request path for /api/*:
    identify client -> local policy lookup (BLOCKED => 429 at once)
    -> token bucket scaled by the client's policy multiplier (Redis, atomic)
    -> forward to backend
    -> queue one telemetry event (in memory, non-blocking)
A background task publishes queued events to a Redis Stream (see telemetry.py).
Behavioural scoring/policy is added in later milestones.

Redis failure behaviour (RATE_LIMIT_FAIL_MODE):
    open   (default) - the request is forwarded without limiting and the
                       response carries "X-RateLimit-Status: bypassed".
    closed           - the request is rejected with 503 Service Unavailable.
Either way the failure is logged. Redis calls have a short timeout so a dead
Redis cannot stall the request path. Telemetry failures never affect requests;
they are counted and later reported to the stream as "gap" events.
"""
import hashlib
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import replace

import httpx
import redis.asyncio as redis_lib
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from app.admin.api import router as admin_router
from app.common.redis_client import close_redis, create_redis, ping
from app.config import Settings, load_settings
from app.gateway.policy_cache import PolicyCache
from app.gateway.telemetry import TelemetryPublisher, build_event
from app.policy.params import State
from app.gateway.token_bucket import BucketResult, TokenBucket

log = logging.getLogger("gateway")

# Headers that must not be copied between hops.
_HOP_BY_HOP = {"connection", "keep-alive", "transfer-encoding", "te", "trailer",
               "upgrade", "proxy-authorization", "proxy-authenticate", "host",
               "content-length"}


def create_http_client(settings: Settings) -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=settings.backend_url, timeout=10.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    app.state.settings = settings
    app.state.redis = create_redis(settings)
    app.state.http = create_http_client(settings)
    # Monitoring reads can be large; they get their own small pool with a patient timeout so they can
    # neither starve nor be starved by the request path's strict 0.5 s limits.
    app.state.admin_redis = create_redis(replace(settings, redis_timeout_s=5.0, redis_max_connections=4))
    app.state.bucket = TokenBucket(app.state.redis, settings.bucket_capacity,
                                   settings.bucket_refill_per_sec)
    app.state.telemetry = TelemetryPublisher(
        app.state.redis, maxlen=settings.telemetry_maxlen,
        queue_size=settings.telemetry_queue_size, tick_s=settings.telemetry_tick_s)
    app.state.telemetry.start()
    app.state.policy = PolicyCache(app.state.redis, settings.policy_refresh_s,
                                   settings.policy_max_age_s)
    try:
        await app.state.policy.refresh()      # best effort: start with current policy if possible
    except (redis_lib.RedisError, OSError) as exc:
        log.warning("initial policy load failed: %s", exc)
    app.state.policy.start()
    yield
    await app.state.policy.stop()
    await app.state.telemetry.stop()
    await app.state.http.aclose()
    await close_redis(app.state.admin_redis)
    await close_redis(app.state.redis)


app = FastAPI(title="Hybrid Smart API Limiter - Gateway", lifespan=lifespan)
app.include_router(admin_router)          # /dashboard and read-only /admin/* (needs ADMIN_TOKEN)


def identify_client(request: Request, settings: Settings) -> str:
    """Who is this client? The answer is the key for the bucket AND the telemetry.

    * X-API-Key present (and, if ALLOWED_API_KEYS is configured, recognised):
      "key:<first 16 hex of SHA-256>". The raw key is never stored or logged.
    * Otherwise the TCP source address: "ip:<addr>".
    X-Forwarded-For is deliberately ignored: any client can forge it. Put a
    trusted-proxy setting here if the gateway is ever deployed behind one.

    An unrecognised key falls back to the IP identity rather than creating a new
    identity, so rotating made-up keys does not hand out fresh token buckets.
    """
    api_key = request.headers.get("x-api-key")
    if api_key:
        digest = hashlib.sha256(api_key.encode()).hexdigest()
        allowed = settings.allowed_key_hashes
        if not allowed or digest in allowed:
            return "key:" + digest[:16]
    return "ip:" + (request.client.host if request.client else "unknown")


def rate_limit_headers(result: BucketResult) -> dict[str, str]:
    headers = {
        "X-RateLimit-Limit": str(result.limit),
        "X-RateLimit-Remaining": str(result.remaining),
        "X-RateLimit-Reset": str(result.reset_s),   # seconds until bucket is full
    }
    if not result.allowed:
        headers["Retry-After"] = str(max(1, result.retry_after_s))
    return headers


@app.get("/healthz")
async def healthz(request: Request):
    """Liveness plus Redis connectivity and telemetry counters."""
    redis_ok = await ping(request.app.state.redis)
    body = {"status": "ok" if redis_ok else "degraded", "redis": redis_ok,
            "telemetry": request.app.state.telemetry.stats(),
            "policy": request.app.state.policy.stats()}
    return JSONResponse(body, status_code=200 if redis_ok else 503)


@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def protected(path: str, request: Request):
    started = time.perf_counter()
    ts_ms = time.time_ns() // 1_000_000
    client_id = identify_client(request, request.app.state.settings)

    response, outcome, up_status = await handle(request, path, client_id)

    request.app.state.telemetry.record(build_event(
        client=client_id, method=request.method, path=f"/api/{path}", ts_ms=ts_ms,
        status=response.status_code, up_status=up_status, outcome=outcome,
        latency_ms=int((time.perf_counter() - started) * 1000)))
    return response


async def handle(request: Request, path: str, client_id: str) -> tuple[Response, str, int]:
    """Returns (response, outcome, upstream_status). upstream_status is 0 if the backend never answered."""
    extra_headers: dict[str, str] = {}
    outcome = "allowed"

    # Policy lookup: a dictionary read, no I/O (see policy_cache.py).
    view = request.app.state.policy.get(client_id)
    if view.state != State.NORMAL:
        extra_headers["X-Policy-State"] = view.state.name
    if view.state == State.BLOCKED:
        retry = request.app.state.settings.blocked_retry_after_s
        return (JSONResponse({"error": "client blocked due to abusive behaviour",
                              "retry_after_seconds": retry}, status_code=429,
                             headers={**extra_headers, "Retry-After": str(retry)}), "blocked", 0)

    try:
        result = await request.app.state.bucket.consume(client_id, multiplier=view.multiplier)
    except (redis_lib.RedisError, OSError) as exc:
        log.error("Redis unavailable, fail mode=%s: %s", request.app.state.settings.fail_mode, exc)
        if request.app.state.settings.fail_mode == "closed":
            return (JSONResponse({"error": "rate limiter unavailable"}, status_code=503,
                                 headers={"Retry-After": "5"}), "rejected", 0)
        extra_headers["X-RateLimit-Status"] = "bypassed"
        outcome = "bypassed"
    else:
        extra_headers.update(rate_limit_headers(result))
        if not result.allowed:
            return (JSONResponse(
                {"error": "rate limit exceeded",
                 "retry_after_seconds": max(1, result.retry_after_s)},
                status_code=429, headers=extra_headers), "limited", 0)

    response, up_status = await forward(request, path, extra_headers)
    if up_status == 0:
        outcome = "bad_gateway"
    return response, outcome, up_status


async def forward(request: Request, path: str, extra_headers: dict[str, str]) -> tuple[Response, int]:
    fwd_headers = {k: v for k, v in request.headers.items() if k.lower() not in _HOP_BY_HOP}
    try:
        upstream = await request.app.state.http.request(
            request.method, f"/api/{path}", params=request.query_params,
            content=await request.body(), headers=fwd_headers)
    except httpx.HTTPError as exc:
        log.error("Backend unreachable: %s", exc)
        return JSONResponse({"error": "bad gateway"}, status_code=502, headers=extra_headers), 0

    # httpx has already decoded gzip/deflate bodies, so the Content-Encoding header must not be repeated.
    headers = {k: v for k, v in upstream.headers.items()
               if k.lower() not in _HOP_BY_HOP and k.lower() != "content-encoding"}
    headers.update(extra_headers)
    return Response(content=upstream.content, status_code=upstream.status_code,
                    headers=headers), upstream.status_code
