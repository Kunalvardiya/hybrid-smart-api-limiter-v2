# Architecture (Phase I deliverable, as implemented in Phase II)

Source of requirements: the project synopsis and presentation ("Hybrid Smart API Limiter"). The key design principle
from those documents: **the telemetry and risk-processing path is asynchronous, while the gateway fast path stays
lightweight and deterministic.**

## Two cooperating paths

```
FAST PATH (synchronous, per request)                         SLOW PATH (asynchronous, continuous)

client ─► Gateway ─► Protected API                           Gateway ──queue──► Redis Stream telemetry:events
           │  ▲                                                                        │ consumer group
 1 identify client                                                                     ▼
 2 policy lookup  ◄── local policy cache ◄── (copied every 1 s)             Behaviour worker
 3 token bucket (Redis Lua, atomic)                                          1-second windows per client
 4 forward / reject (429)                                                    EWMA baseline, D B F C
 5 queue one telemetry event (no I/O)                                        Risk = 100·(.25D+.25B+.25F+.25C)
                                                                             Policy engine: state machine
              Redis Policy Store  ◄─────────────────────────────────────────  policy:{client}, policy:enforced
```

If the worker is down, the gateway keeps enforcing the plain token bucket. If Redis is down the gateway follows
`RATE_LIMIT_FAIL_MODE` (`open` default / `closed`).

## Components

| Component | Code | Responsibility |
|---|---|---|
| API gateway | `app/gateway/main.py` | Identify client, apply policy + token bucket, proxy to the protected API, record telemetry |
| Token bucket | `app/gateway/token_bucket.py` | Deterministic limiter; one Lua script does refill + spend atomically on the Redis clock |
| Telemetry publisher | `app/gateway/telemetry.py` | In-memory queue → batched `XADD`; counts and reports lost events (gap events) |
| Policy cache | `app/gateway/policy_cache.py` | Local dictionary of non-NORMAL clients, refreshed in the background; staleness rules |
| Behaviour worker | `app/worker/consumer.py` | Redis Streams consumer group, acks, stores results, heartbeat |
| Behaviour engine | `app/worker/engine.py`, `features.py`, `scoring.py`, `params.py` | Windows, EWMA, D/B/F/C, risk score (pure, no Redis) |
| Policy engine | `app/policy/` | State machine (thresholds, hysteresis, dwell, recovery), wire format of the policy table |
| Admin API + dashboard | `app/admin/` | Read-only monitoring (`/admin/*`, `/dashboard`), token protected |
| Protected demo API | `app/backend/main.py` | Stand-in for the API being protected; can produce 4xx on demand |
| Load scenarios | `load/` | Locust scenarios + Redis-side verifier |
| No-Docker stand-in | `tools/dev_stack.py` | Single-process stack on fakeredis for experiments |

## Redis data

| Key | Type | Writer → Reader | Content |
|---|---|---|---|
| `bucket:{client}` | hash | gateway (Lua) | tokens, timestamp; expires when idle |
| `telemetry:events` | stream | gateway → worker | one event per request, plus gap events (`docs/TELEMETRY.md`) |
| `policy:{client}` | hash | worker → admin API | latest score record + state (`docs/POLICY.md`) |
| `policy:enforced` | hash | worker → gateway | clients that are not NORMAL: `STATE:multiplier:updated_ms` |
| `policy:events` | stream | worker → admin API | every state change with its reason |
| `risk:log` | stream | worker → verifier | history of score records |
| `worker:heartbeat` | hash | worker → admin API | liveness and counters, expires after 30 s |

## Key design decisions

* **Client identity** is a hashed `X-API-Key` or the source IP; `X-Forwarded-For` is ignored (forgeable).
  Risks are listed in `docs/TELEMETRY.md`.
* **Event time, not wall time, in the worker.** Windows are the gateway's one-second ids; a window is evaluated
  only after the stream has moved 2 s past it, so silence from a broken telemetry path never looks like "quiet". The gateway's 1-second heartbeat (`tick`) events keep time moving when the system is genuinely idle (`docs/SCORING.md`).
* **Missing data never produces a score.** Cold start, restarts and reported telemetry gaps yield `incomplete`.
* **Enforcement is progressive and gradual**: escalate immediately, de-escalate one level at a time after the
  dwell time (`docs/POLICY.md`).
* **Everything numeric that the documents do not specify is a configurable implementation choice**, listed in
  `docs/SCORING.md` and `docs/POLICY.md`.

## Implementation plan that was followed

1. Scaffold, configuration, FastAPI apps, Docker Compose, Redis connection.
2. Token bucket (Lua) and gateway integration.
3. Telemetry collection (queue, stream, gap reporting).
4. Behaviour worker: windows, EWMA, D/B/F/C, risk score.
5. Policy engine: states, hysteresis, dwell, recovery; gateway policy cache.
6. Full-flow integration tests.
7. Locust scenarios + verifier.
8. Dashboard.
9. Phase II completion review (`docs/PHASE2_REVIEW.md`).
