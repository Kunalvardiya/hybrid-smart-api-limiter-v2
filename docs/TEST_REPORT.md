# Phase II test report

**Final run (25 Oct 2026): 207 passed, 0 failed** with the in-memory Redis stand-in (`fakeredis`), about 70 s.
**The same suite against the real Redis 7 container: 206 passed, 1 skipped, 0 failed** (the skipped test toggles the
fake server's connectivity flag and has no real-Redis equivalent). Both runs were repeated several times during the
review; results were stable.

| Test file | Tests | Covers |
|---|---|---|
| test_scoring_unit | 41 | EWMA, D/B/F/C normalisation, risk formula, edge cases, parameters |
| test_policy_machine | 33 | thresholds, hysteresis, dwell, recovery, boundaries, wire format |
| test_telemetry | 20 | event schema, windows, privacy, loss/gap reporting, heartbeat ticks |
| test_engine | 19 | windows, baseline, warm-up, gaps, ramp-in, silent clients, determinism |
| test_integration_flow | 18 | whole flow: legitimate, burst, 4xx, concentrated, concurrency, errors, non-blocking |
| test_admin | 21 | dashboard API: auth, counts match reality, health, failures, privacy |
| test_token_bucket | 15 | atomicity, refill, capacity, multiplier |
| test_gateway_limiter | 10 | limiter in the request path, headers, Redis failure modes, compressed upstreams |
| test_gateway_policy | 10 | policy cache, enforcement per state, staleness |
| test_worker_policy | 6 | stream → score → state → table, recovery, restart, gaps, idle systems |
| test_worker_consumer | 7 | consumer group, acks, outbox, restart |
| test_milestone1 | 7 | configuration, health, Redis pool |

## Defects found and fixed during the Phase II completion review

| # | Defect | How it was found | Fix | Regression test |
|---|---|---|---|---|
| 1 | Burst of simultaneous requests made the Redis client raise *Too many connections*; the gateway counted that as a Redis failure and, in fail-open mode, let the requests through **unlimited** exactly when a burst hit | running the suite against real Redis (fakeredis does not model pools) | blocking connection pool, size 50, waits for a free connection (`app/common/redis_client.py`) | pool test + the 300-request concurrency test on real Redis |
| 2 | Gzip-compressed upstream responses were returned decoded but still labelled `Content-Encoding: gzip` | code review | drop the stale header (`app/gateway/main.py`) | `test_compressed_upstream_responses_...` |
| 3 | Monitoring queries (large `XREVRANGE`) timed out against the gateway's strict 0.5 s Redis timeout and would block the event loop while parsing | calling the dashboard API on the Docker stack | separate small admin Redis pool with a 5 s timeout, default window cap 5 000, `hiredis` in the image | admin tests + live check (8 117 events in 87 ms) |
| 4 | A restricted client that **never built a baseline** (for example a scanner) kept a "warming up" score after it went silent and could **never be released** | the Docker scenario left three clients MONITORED indefinitely | zero requests in the span means D = 0 for any baseline, so the score is complete (`app/worker/engine.py`) | two regression tests (fail without the fix) |
| 5 | When the whole system was idle, time did not advance in the worker, so restricted clients stayed restricted until new traffic arrived (recovery requirement not met for an idle system) | same Docker observation | gateway heartbeat `tick` events once a second (`TELEMETRY_TICK_S`); never counted as data or loss | three tests, including "no ticks and no traffic releases nothing" |
| 6 | The backend demo API was published on a host port, so clients could bypass the gateway | review | not published any more | n/a (compose check) |
| 7 | `.env.example` shipped a guessable placeholder password | review | empty value; Compose refuses to start until a real one is set | n/a |

Earlier defects found by realistic traffic (before the review) are listed in `docs/LOAD_TESTING.md`.
## Reproduce

```bash
.\.venv\Scripts\python.exe -m pytest -q                       # everything (~70 s)
.\.venv\Scripts\python.exe -m pytest tests/test_integration_flow.py -v   # full-flow tests only
```

Against a real Redis (scratch database 15 — the tests **flush it**):

```bash
docker compose up -d redis
$env:REAL_REDIS_URL="redis://:YOUR_REDIS_PASSWORD@localhost:6379/15"; .\.venv\Scripts\python.exe -m pytest -q
```

## How the integration tests work (`tests/flow_harness.py`)

The real gateway app (policy cache, telemetry publisher, token bucket), the demo backend, the behaviour
worker, scoring engine and policy engine all run in one process. Time is simulated: the gateway's request
timestamps and the token bucket's clock are driven by the test, so minutes of traffic take seconds. Once per
simulated second the harness flushes telemetry, runs the worker and refreshes the gateway's policy cache,
mirroring the live loops. The code under test is not mocked; only the clock and the transport are.

## Scenarios (18 integration tests)

| Area | What is verified | Status |
|---|---|---|
| Legitimate traffic | 300 requests all `200`; baseline learns 5.0 req/s; score 0; state NORMAL; no restriction | passed |
| Burst | 50 req/s → score 50 (D=1, B=1) → THROTTLED; gateway applies bucket 20×0.25=5; stops → MONITORED → NORMAL; full bucket restored | passed |
| Repeated 4xx | 404s across 10 endpoints → F=1, C=0 → score 25 → MONITORED, not restricted; one failing endpoint (credential stuffing) → F=1, C=1 → 50 → THROTTLED | passed |
| 429 ≠ failure | Rate-limited requests never raise F | passed |
| Concentrated endpoint | Normal-volume single endpoint → MONITORED only; flood on one endpoint (D=B=C=1) → score 75 → BLOCKED; 429 + `Retry-After`; stays blocked while abuse continues; stops → THROTTLED → MONITORED → NORMAL; BLOCKED lasted ≥ 60 s | passed |
| Redis consistency | events == requests; no pending stream entries; stored score == 25·(D+B+F+C); enforcement table matches per-client hashes and multipliers; `policy:events` forms an unbroken chain per client | passed (asserted in 6 scenarios) |
| Concurrency | 3 clients × 100 simultaneous requests: exactly 20 allowed each (full stack); one client's policy does not affect another | passed |
| Error handling | Redis outage: fail-open, exactly the lost events reported in one `gap`, no state change; backend down → 502, not counted as client failure; gateway works with no worker; malformed telemetry skipped | passed |
| Non-blocking | Publisher stalled / policy refresh hung: 100-request timing within 3× of normal; queue overflow answers every request and the loss is reported (recorded + reported = sent) | passed |
| Real-clock smoke | publish + consume + score latency per batch < 0.5 s; cold start correctly reported `incomplete` | passed |

## Findings from the integration-test round (before the review)

* The integration tests found no defect in the shipped code; six failures or hangs during development were test
  mistakes, each analysed first.
* A realistic Locust run (not the simulated traffic) later exposed a baseline ramp-in defect, fixed with regression
  tests (`docs/LOAD_TESTING.md`).
* **Design limitation, not a defect:** a client with no history cannot reach BLOCKED by volume and concentration alone
  (D needs a baseline, so B + C = 50 → THROTTLED; `test_attacker_without_history_is_capped_at_throttled`).

## Verified on the real stack (Docker, real Redis 7), 25 Oct 2026

* `docker compose up --build`: all four services start; `redis` and `gateway` report healthy; backend not reachable from the host.
* The whole test suite against the real Redis container (above).
* The `multi_identity` Locust scenario run twice against the containers (8 117 requests before the last fixes, 8 447 on the
  final code): telemetry events = requests sent (100 %), zero requests bypassing the limiter, 14/14 clients scored, flooder
  THROTTLED → BLOCKED → recovered to NORMAL (about 190 s after the start), scrapers MONITORED and recovered after the load stopped.
* Dashboard API totals equal Locust's counters exactly (8 117 / 2 786 throttled-or-blocked / 2 234 upstream 4xx).
* Dashboard page loaded in a browser against the Docker stack and showed live data.

## Not tested / not verified

* **Docker for the other five scenarios** (`normal`, `flash_crowd`, `sustained_high`, `low_and_slow`, `randomized`): run on the
  in-memory stand-in only.
* **A clean-machine setup**: the README commands were executed piece by piece (fresh virtual environment from
  `requirements-dev.txt` passed 103 tests; `.env` generation snippet checked on a copy), but not on a different computer.
* **Several gateway instances** sharing one Redis, and clock skew between them.
* **Worker crash mid-batch**, and stream trimming deleting unread events when the worker lags.
* **Redis restart** (policy state is lost: the container runs without persistence).
* **Throughput, latency, CPU and memory overhead** (Phase III evaluation work). The only timing evidence is the
  non-blocking tests (stalled publisher / policy refresh) and a real-clock latency smoke test.
* **Threshold quality**: thresholds and parameters are unvalidated implementation choices.
* **Browsers** other than the embedded one; **TLS**; **request-body limits**; **brute-force protection** of the admin token.