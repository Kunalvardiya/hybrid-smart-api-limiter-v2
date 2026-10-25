# Locust traffic scenarios (Phase II demonstration)

These scenarios generate controlled traffic against the **local** gateway so you can watch telemetry,
scoring and policy transitions happen. They are demonstrations and sanity checks for Phase II.
They are **not** the Phase III comparison against a static limiter.

Locust only proves that requests were *sent*. Every scenario therefore has a verifier
(`load/observe.py`) that reads Redis afterwards and checks that the requests were recorded as
telemetry, scored, and moved through policy states, and that the scenario's **predictions** held.

## Safety

* `load/common.py` refuses to start unless the target host is `localhost`, `127.0.0.1` or `::1`
  (checked at start-up and again when a test starts, so the web UI cannot bypass it).
* At most 200 Locust users; each user is capped at 100 requests/s (`MAX_USER_RATE`).
* All services bind to `127.0.0.1`. API keys are synthetic (`lt-<scenario>-<run>-<role>-<n>`).
* The scenarios only write rows keyed by those synthetic identities. Nothing is deleted.
* Do not point Locust at any other machine.

## 1. Start the application

**Real stack (Docker Desktop running):**

```bash
docker compose up --build -d
```

**No Docker (development stand-in, see limitations):**

```bash
.\.venv\Scripts\python.exe -m tools.dev_stack
```

Leave that terminal open. It starts the gateway (8000), backend (9000), worker, and an in-memory Redis
stand-in on 6390, as separate processes.

## 2. Run a scenario

Use a **new run id every time** (baselines and policy state live in Redis per identity).

```bash
# Docker stack
.\load\run_scenario.ps1 -Scenario flash_crowd -RunId demo1

# no-Docker stand-in
.\load\run_scenario.ps1 -Scenario flash_crowd -RunId demo1 -RedisUrl redis://127.0.0.1:6390/0
```

The script runs Locust headless for the scenario's duration, then runs the verifier and prints a report.
Scenarios: `normal`, `flash_crowd`, `sustained_high`, `low_and_slow`, `randomized`, `multi_identity`.

With the Locust web UI instead (see live charts at http://localhost:8089):

```bash
$env:SCN_RUN_ID="demo1"
.\.venv\Scripts\python.exe -m locust -f load/scenarios/flash_crowd.py --host http://127.0.0.1:8000
# in the UI: users = 35 (see table), spawn rate = 35, then after it ends:
.\.venv\Scripts\python.exe -m load.observe --scenario flash_crowd --run-id demo1
```

### Configuration (environment variables)

| Variable | Default | Effect |
|---|---|---|
| `SCN_RUN_ID` | `r1` | identity suffix; **change per run** |
| `SCN_SEED` | `1` | seeds every identity's RNG: same seed + run id ⇒ same request sequence plan |
| `SCN_TIME_SCALE` | `1.0` | multiplies every phase duration (use `0.5` for a faster, shorter run) |
| `SCN_RATE_SCALE` | `1.0` | multiplies every request rate |
| `SCN_COUNT_<ROLE>` | per scenario | number of identities of a role, e.g. `SCN_COUNT_CROWD=10` |
| `MAX_USER_RATE` | `100` | per-user safety cap (req/s) |

Rates, durations and identities are defined in one place, `load/definitions.py`.
The Locust `-u` value must be at least the scenario's total users (printed by `run_scenario.ps1`).
Reproducibility note: seeds fix *what each identity plans to do*; real request timing still varies
with machine load, so counts differ slightly between runs.

## 3. Scenarios

| Scenario | Users | Run time | What it does | The design predicts |
|---|---|---|---|---|
| `normal` | 5 | 150 s | 5 clients, ~2 req/s each, mixed endpoints | Everyone stays NORMAL, no 429 |
| `flash_crowd` | 35 | 180 s | 5 steady + 30 new identities arriving over ~15 s, normal behaviour for 60 s, then leaving | Aggregate load ×8, nobody throttled or blocked, no 429 for the crowd |
| `sustained_high` | 3 | 170 s | `ramper` ×2: 70 s normal then 100 s at 25 req/s; `cold`: 25 req/s from the start | `ramper` → THROTTLED (D=1, B=1 = 50) with 429s; `cold` → only MONITORED (no baseline ⇒ D unavailable) |
| `low_and_slow` | 2 | 170 s | `reader`: 0.5 req/s to one endpoint; `stuffer`: 1.5 req/s failing logins | `reader` evades detection (too few requests per 10 s for F/C); `stuffer` reaches ≥ MONITORED |
| `randomized` | 2 | 240 s | Bots whose rate (0.5-30 req/s), endpoints and jitter change every 8-20 s (seeded) | Scoring reacts (≥ MONITORED) and hysteresis/dwell keep transitions ≤ 14 per bot |
| `multi_identity` | 14 | 200 s | 10 normal, 3 scrapers (4 req/s of 404s), 1 flooder (60 s normal then 60 req/s at one endpoint) | Normal users unaffected; each scraper MONITORED (F=1) only; flooder BLOCKED |

## 4. What to observe

In **Locust** (headless summary or web UI): requests/s, failures, response-time percentiles. Locust
treats 200 and 429 as success (429 is the limiter working) and 401/404 as expected only for the
scenarios that generate them; anything else (5xx, connection errors) is a failure. A rising 429 count
together with a falling 200 count for one identity means enforcement is biting.

In **Redis / the verifier report**:

| Observation | Where | What it tells you |
|---|---|---|
| Events recorded vs requests sent | stream `telemetry:events` vs Locust counters | telemetry collection is complete (expect ≈100 %) |
| Score records per identity | stream `risk:log` | the worker is scoring |
| Max D / B / F / C and score per identity | `risk:log` | *which signal* drove the decision |
| State changes and their reasons | stream `policy:events`, hash field `last_transition` | thresholds, hysteresis and dwell at work |
| Current restrictions | hash `policy:enforced` | who is being limited right now |
| `X-Policy-State`, `X-RateLimit-*`, 429 + `Retry-After` | response headers (Locust counters in the JSON) | the gateway applied the policy |
| Gap events, pending entries | `telemetry:events` / `XPENDING` | data loss or a lagging worker |

Quick manual look while a scenario runs (Docker):

```bash
docker compose exec redis sh -c 'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning HGETALL policy:enforced'
docker compose exec redis sh -c 'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning XREVRANGE policy:events + - COUNT 10'
```

## 5. Observations (actual runs, 25 Oct 2026)

**Where these ran:** the no-Docker stand-in (`tools.dev_stack`: gateway, in-process backend and worker in one
process, fakeredis), real HTTP from Locust, real clock, default parameters. Latency and throughput from this stand-in are
meaningless; only the behaviour is informative. The multi_identity scenario was **also run against the Docker stack
(real Redis)**, see the next section; the other five scenarios have only been run on the stand-in.
The verifier checked, for every run: telemetry events recorded = requests sent (100 % in all six runs),
scoring records for every identity, nothing pending in the stream, no gap events.

| Scenario (run id) | Requests | What was observed | Prediction |
|---|---|---|---|
| `normal` (n2) | 1 457 | 5/5 NORMAL, max score ~9, zero 429s | held |
| `flash_crowd` (f2) | 5 329 | 35/35 NORMAL, zero 429s, zero transitions | held |
| `sustained_high` (h1) | 7 403 | `ramper` ×2: MONITORED → THROTTLED, score 50 (D=1, B=1), 3 287 of 4 149 requests got 429. `cold`: MONITORED only (score 25, B=1, D unavailable), 1 535 of 3 254 got 429 | held |
| `low_and_slow` (s1) | 320 | `stuffer`: MONITORED → THROTTLED (F=1, C=1). `reader` flickered into MONITORED twice (score up to 34.5) | **reader prediction FAILED** |
| `low_and_slow` (s2) | 365 | `stuffer` THROTTLED again, only 6 of 280 requests got 429. `reader` stayed NORMAL (max score 3.6) | held (revised prediction) |
| `randomized` (x1) | 2 798 | Both bots reached BLOCKED (score up to 100, all four features saturated); 16 transitions in total (7-9 per bot, ≤ 14 each), 2 260 of 2 798 requests got 429 | held |
| `multi_identity` (m1) | 8 121 | 10 normal users stayed NORMAL (max score 10.1, zero 429s). 3 scrapers each MONITORED (F=1, 25 points), zero 429s. Flooder: THROTTLED → BLOCKED (score 75) → THROTTLED → MONITORED → NORMAL, i.e. it blocked **and recovered** inside the run; 2 869 of 3 036 requests got 429 | held |

### The same scenario on the Docker stack (real Redis 7 containers)

`multi_identity` was run twice against `docker compose` (8 117 requests before the last fixes, 8 447 on the final code).
Both runs passed every check: telemetry events = requests sent (100 %), **0 requests bypassing the limiter**, 14/14
identities scored, 10 normal users NORMAL, the 3 scrapers MONITORED, the flooder blocked and recovered. Recorded
timeline of the final run (seconds after the start):

| t | Event |
|---|---|
| 1 s | the 3 scrapers NORMAL → MONITORED (F = 1, score 25) |
| 60 s / 61 s | flooder NORMAL → MONITORED (48.3) → THROTTLED (66.6) |
| 70 s | flooder THROTTLED → BLOCKED (score 75.0: D = B = C = 1) |
| 148 s | flooder BLOCKED → THROTTLED (flood ended at 140 s; BLOCKED dwell 60 s long served) |
| 178 s | THROTTLED → MONITORED (dwell 30 s) |
| 188 s | MONITORED → NORMAL (dwell 10 s), access fully restored |
| 210 s | the 3 scrapers MONITORED → NORMAL, after the load generator had finished (idle system, heartbeat ticks only) |

This run also exposed two defects that the stand-in runs had hidden (a restricted client that never built a baseline stayed
restricted forever once silent, and an idle system did not advance time); both are fixed and documented in
`docs/TEST_REPORT.md`.

### Things the runs taught us (including mistakes)

1. **A real defect, found by the flash-crowd scenario and fixed.** The first run (f1, before the fix) pushed 2 of 35
   *legitimate* identities into MONITORED with D = 1.0 and gave most newcomers D of 0.2-0.7. Cause: while a new
   client's first 10 s were being filled, the seconds before it existed were averaged in as silence, so its
   baseline started near zero and its real rate then looked like a 3× deviation. The simulated tests had missed this
   because their traffic was perfectly regular. Fix: a new client's rate is averaged over the seconds it has actually
   existed, and the baseline only learns once a full span has been seen
   (`app/worker/engine.py`; two regression tests, one verified to fail without the fix). After the fix (f2) the same
   scenario produced zero transitions.
2. **A refuted prediction.** I predicted the 0.5 req/s poller (`reader`) would never be flagged. In run s1 random
   timing occasionally put ≥ 10 requests inside one 10 s span, so C = 1 and it flickered into MONITORED (hysteresis and
   dwell kept it from flapping faster). In s2 it stayed NORMAL. The honest summary: low-and-slow polling is detected
   *intermittently by chance*, and never restricted. The prediction in `load/definitions.py` was revised after the fact,
   with this explanation recorded next to it; s2 is a fresh run against the revised version.
3. **THROTTLED barely touches slow bots.** The credential stuffer (1.5 req/s) was THROTTLED in both runs but received
   almost no 429s (6 of 280), because the throttled bucket (5 tokens, 2.5/s) is still above its request rate.
   Throttling limits *volume*; it does not stop a slow bot, and the stuffer cannot reach BLOCKED (D needs a baseline
   it never builds, so its maximum is F + C = 50).
4. **Discarded runs.** My first attempt at the stand-in ran gateway and worker as separate processes over
   fakeredis's TCP server. Its logs showed the token-bucket script failing with "connection closed" on over half of the
   requests, so the limiter was failing open while Locust happily reported success. Those runs were thrown away and
   the stand-in was rebuilt in-process. This is why the verifier now counts requests that bypassed the token bucket
   (`X-RateLimit-Status: bypassed`) and fails the run if there are any.
5. **Time scaling breaks predictions.** `SCN_TIME_SCALE=0.3` smoke run (q3): the flooder only reached THROTTLED
   because the scorer's warm-up and dwell times are real seconds and were not scaled. Use `SCN_TIME_SCALE` only for
   quick plumbing checks, not for checking predictions.
6. **Randomised timing is not repeatable to the request.** Seeds fix what each identity *plans* to do; actual timing
   depends on machine load, so two runs of one scenario differ slightly (compare s1 and s2).

### Limitations

* Only multi_identity was run against Docker. Throughput and latency under load remain unmeasured (Phase III).
* `normal`/`flash_crowd` identities share one machine and one core with the stand-in; heavy scenarios may be limited
  by the stand-in's speed, not the design.
* Distributed scrapers are judged identity by identity; nothing correlates the three. That is by design (Phase II).
* The scenarios exercise the system; they do not prove the thresholds are good. Quantifying false positives, missed
  attacks and the trade-offs against a static limiter is Phase III.
* Locust prints a harmless `StatsCSVFileWriter ... I/O operation on closed file` message at shutdown when `--csv` is used
  (a Locust shutdown race; the summary and our own JSON counters are unaffected).
* The verifier reads whole streams; it is meant for runs of up to a few hundred thousand events.
