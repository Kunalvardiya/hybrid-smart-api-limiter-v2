# Demonstrating the project to evaluators

A 12-15 minute demo that shows every Phase II requirement working on the real stack (Docker), with honest limits.
Practise it once beforehand: the main scenario takes about 3.5 minutes of wall-clock time.

## Before the session (10 minutes earlier)

1. Start **Docker Desktop**, wait for *Engine running*.
2. In the project folder:

   ```powershell
   docker compose up --build -d
   docker compose ps                  # redis + gateway healthy, backend + worker Up
   curl.exe http://127.0.0.1:8000/healthz
   ```
3. Open the dashboard <http://127.0.0.1:8000/dashboard>, log in with `ADMIN_TOKEN` from `.env`, keep it on a second
   screen or window. Confirm it shows *Redis OK* and *Worker running*.
4. Have three PowerShell windows ready in the project folder: **A** (commands), **B** (scenario), **C** (Redis peek).
5. Use a **new RunId** (`demo1`, `demo2`, ...) for every scenario run: baselines persist in Redis per identity.
6. Open `docs/POLICY.md`, `docs/SCORING.md` and the source presentation in case a question needs the slide.

Backup plan if Docker misbehaves: `.\.venv\Scripts\python.exe -m tools.dev_stack` (token `dev-admin`) runs the same
code on an in-memory Redis stand-in. Say clearly that it is a stand-in.

## The story (what to say and show)

### 1. The problem and the idea (1 min)
"Fixed limits treat a legitimate burst and an attack the same. We keep a deterministic token bucket for baseline
protection and add a lightweight, explainable behavioural risk score on a separate asynchronous path."
Show the diagram in `README.md` / `docs/ARCHITECTURE.md`: fast path vs slow path.

### 2. The deterministic limiter (2 min) — window A
```powershell
curl.exe -i http://127.0.0.1:8000/api/ping -H "X-API-Key: demo"
1..60 | ForEach-Object { curl.exe -s -o NUL -w "%{http_code} " http://127.0.0.1:8000/api/ping -H "X-API-Key: demo" }
```
Point out `X-RateLimit-Limit/Remaining/Reset`, then the `429` with `Retry-After` once the 20-token bucket is empty,
refilling at 10/s. Explain: one Redis Lua script does refill + spend atomically, so concurrent requests cannot
overspend (tested with 100 simultaneous requests).

### 3. Telemetry through Redis Streams (1 min) — window C
```powershell
$pw = ((Get-Content .env | Where-Object { $_ -like 'REDIS_PASSWORD=*' }) -split '=',2)[1]
docker compose exec redis redis-cli -a $pw --no-auth-warning XREVRANGE telemetry:events + - COUNT 30
```
Entries with `type tick` are the gateway's once-a-second heartbeat (it lets the system recover when idle); scroll to one
with `type req`. Show that event: client label (hashed key, never the raw key), one-second window id `win`, normalised endpoint,
`up_status`, outcome. "The gateway only puts the event on an in-memory queue; a background task writes it to the stream, so
telemetry never slows a request, and any loss is counted and reported as a gap event."

### 4. The main demonstration: `multi_identity` scenario (about 4 minutes) — window B + dashboard
```powershell
.\load\run_scenario.ps1 -Scenario multi_identity -RunId demo1
```
14 clients: 10 ordinary users, 3 scrapers (404s), 1 flooder. Narrate against the dashboard. This is the timeline
recorded on the Docker stack (seconds after the start; yours will be within a few seconds):

| t | What happens | What the dashboard shows |
|---|---|---|
| 0-60 s | everyone behaves; baselines are learned | 10 users NORMAL (scores 0-10); the 3 scrapers turn **MONITORED at ~1 s** (F = 1: every answer is a 4xx, score 25) |
| 60 s | the flooder jumps from 2 to 60 req/s at one endpoint | request chart grows; first **429**s |
| 60-61 s | score 48 → 66 | flooder **MONITORED → THROTTLED** (policy update row appears with D, B, F, C and the reason) |
| 70 s | score 75: D = 1, B = 1, C = 1 | flooder **BLOCKED**; "rejected: client BLOCKED" counter rises; other clients unaffected |
| 140 s | the flood stops | blocked requests stop |
| 148 s | span clears, BLOCKED dwell (60 s) already served | **BLOCKED → THROTTLED** |
| 178 s | THROTTLED dwell (30 s) | **THROTTLED → MONITORED** |
| 188 s | MONITORED dwell (10 s) | **MONITORED → NORMAL** (access fully restored) |
| 210 s | load generator finished; only heartbeats remain | the 3 scrapers **recover to NORMAL** with no traffic at all |

Explain while it runs:
* `Risk = 100 × (0.25D + 0.25B + 0.25F + 0.25C)`: click a client row, read D/B/F/C, note which feature drove the score.
* **Hysteresis:** leave MONITORED only below 20 (enter at 25), THROTTLED below 45 (enter at 50), BLOCKED below 70
  (enter at 75). **Dwell:** 10/30/60 s before a state can be left. **Recovery:** one level at a time.
* **No false alarm for a crowd:** (optional, 3 min) run `-Scenario flash_crowd -RunId demo2`: 30 newcomers arrive and nobody is restricted.

When the run ends the script prints a verification report: telemetry events = requests sent (100 %), every client scored,
the predicted states reached, **0 requests bypassed the limiter**. "Locust only shows requests were sent; this checks Redis."

### 5. Explainability and storage (1 min) — window C
```powershell
docker compose exec redis redis-cli -a $pw --no-auth-warning XREVRANGE policy:events + - COUNT 3
docker compose exec redis redis-cli -a $pw --no-auth-warning HGETALL policy:enforced
```
Every state change stores the score, the four features and a plain-language reason (for example
`score 75.0 >= 75 (enter BLOCKED)` or `score 0.0 < 20 and dwell 850s >= 10s (leave MONITORED)`).

### 6. Failure handling (2 min, optional but impressive) — window A
```powershell
docker compose stop worker
1..30 | ForEach-Object { curl.exe -s -o NUL -w "%{http_code} " http://127.0.0.1:8000/api/ping -H "X-API-Key: demo2" }
```
The limiter keeps working without the worker (deterministic baseline protection); the dashboard worker card turns
*stale* then *no heartbeat*. Restart with `docker compose start worker`. Explain the Redis-down policy:
`RATE_LIMIT_FAIL_MODE=open` (default: availability first, requests marked `bypassed`) or `closed` (503).

### 7. Automated evidence (1 min)
```powershell
.\.venv\Scripts\python.exe -m pytest -q
```
About 70 seconds; show the final line (207 passed). Mention that the same suite also passes against the real Redis
container, and show `docs/TEST_REPORT.md`.

### 8. Honest limitations and what comes next (1 min)
* Thresholds and parameters not in the documents are **implementation choices**, configurable and documented; they
  are **not yet tuned**. Calibrating them and comparing against a static limiter is **Phase III**.
* A client with no history cannot exceed THROTTLED by volume alone (D needs a baseline); throttling barely affects very slow bots.
* Low-and-slow polling below ~10 requests per 10 s is mostly invisible by design.
* Identity is an API-key label or IP, not authentication; distributed clients are not correlated.
* Not measured yet: overhead, latency and CPU/memory (Phase III).

## Questions evaluators may ask

| Question | Short answer (and where) |
|---|---|
| Why not machine learning? | The synopsis targets a lightweight, explainable score; the formula is the one in the document with fixed 0.25 weights (`docs/SCORING.md`) |
| Are the 0.25 weights learned? | No, fixed per the synopsis. Correlation-based weights are a possible Phase III experiment |
| What if Redis dies? | Fail-open by default with explicit marking, or fail-closed; the gateway never hangs (0.5 s timeouts) (`app/gateway/main.py`) |
| How do you avoid blocking legitimate bursts? | Escalation needs sustained evidence over a 10 s span; the flash-crowd scenario shows 30 new users unrestricted |
| What prevents flapping? | Hysteresis (5 points) and per-state dwell times; scenario `randomized` stays within 14 transitions per bot |
| What is asynchronous? | Everything after the request: telemetry write, scoring, state decisions; the gateway only reads a local dictionary (`docs/POLICY.md`) |
| How do you know telemetry is complete? | Lost events are counted and written as gap events; windows overlapping a gap are `incomplete`, never scored low |
| What is untested? | See "Not yet verified" in `docs/PHASE2_REVIEW.md` |

## After the demo
```powershell
docker compose down
```
