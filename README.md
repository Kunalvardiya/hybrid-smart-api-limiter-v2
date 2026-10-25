# Hybrid Smart API Limiter

An adaptive, behaviour-based rate limiter and abuse detector (SIT Tumkur, V-semester mini project, batch B10).
A deterministic Redis **token bucket** protects the API on the fast path. Request telemetry flows through a
**Redis Stream** to a worker that scores each client's behaviour with an interpretable formula
`Risk = 100 × (0.25·D + 0.25·B + 0.25·F + 0.25·C)` (rate **D**eviation, **B**urst, **F**ailure rate, endpoint
**C**oncentration, against an EWMA baseline). The score moves the client through
**NORMAL → MONITORED → THROTTLED → BLOCKED** with hysteresis, minimum dwell times and gradual recovery, and the
gateway applies that policy.

**Scope:** Phase I (architecture/requirements) and Phase II (implementation). Phase III (experiments against a static
limiter, evaluation, final report) is **not** part of this repository yet.

```
client ─► gateway ─► protected API                      gateway ─► Redis Stream ─► behaviour worker
           │ policy lookup (local cache)                                              │ windows, EWMA, D B F C, score
           │ token bucket (Redis Lua)                                                 ▼ state machine
           └ telemetry (async queue, no I/O)            gateway ◄── policy table (Redis) ◄── enforcement state
```

| Read this | For |
|---|---|
| [docs/PHASE2_REVIEW.md](docs/PHASE2_REVIEW.md) | **Completion review: requirement-by-requirement status, defects found, checklist** |
| [docs/DEMO.md](docs/DEMO.md) | **How to demonstrate the project to evaluators** |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, Redis keys, design decisions |
| [docs/TELEMETRY.md](docs/TELEMETRY.md) · [SCORING.md](docs/SCORING.md) · [POLICY.md](docs/POLICY.md) | Event schema, score maths and parameters, state machine and thresholds |
| [docs/LOAD_TESTING.md](docs/LOAD_TESTING.md) · [DASHBOARD.md](docs/DASHBOARD.md) · [TEST_REPORT.md](docs/TEST_REPORT.md) | Locust scenarios, monitoring dashboard, test report |

---

## 1. Prerequisites

| Tool | Needed for | Verified version |
|---|---|---|
| Windows 10/11 + PowerShell | commands below are PowerShell | Windows 11 |
| **Python 3.12 or newer** | tests, Locust, tools | 3.14.2 (3.12/3.13 also fine) |
| **Docker Desktop** (with Compose v2) | the real stack: Redis + gateway + worker | Docker 29.7, Compose 5.5 |
| Git | version control | 2.51 |
| A browser | dashboard | Chromium-based browser |

Docker is only needed to run the real stack. The automated tests and a single-process stand-in work without it.

## 2. Setup (once)

Open PowerShell in the project folder.

```powershell
# 1. Python environment and dependencies (tests + Locust + tools)
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt

# 2. Local configuration with fresh random secrets (.env is git-ignored; never commit it)
copy .env.example .env
$rand = { -join ((48..57)+(65..90)+(97..122) | Get-Random -Count 32 | ForEach-Object {[char]$_}) }
(Get-Content .env) -replace '^REDIS_PASSWORD=.*', "REDIS_PASSWORD=$(& $rand)" -replace '^ADMIN_TOKEN=.*', "ADMIN_TOKEN=$(& $rand)" | Set-Content .env -Encoding ascii
```

`REDIS_PASSWORD` is mandatory (Docker Compose refuses to start without it). `ADMIN_TOKEN` enables the dashboard
data; leave it empty and the dashboard/admin API stay disabled. Optional tuning variables are listed in
`.env.example` and documented in `docs/SCORING.md` and `docs/POLICY.md`.

## 3. Start the system (Docker)

1. Start **Docker Desktop** and wait until it says *Engine running*.
2. Build and start everything:

   ```powershell
   docker compose up --build -d
   docker compose ps
   ```

   Expected: `redis` and `gateway` **healthy**, `backend` and `worker` **Up**.
3. Check it:

   ```powershell
   curl.exe http://127.0.0.1:8000/healthz
   curl.exe -i http://127.0.0.1:8000/api/ping -H "X-API-Key: demo"
   ```

   `healthz` should show `"redis":true`. The second call returns `200` with `X-RateLimit-*` headers.
4. Open the dashboard: <http://127.0.0.1:8000/dashboard> and paste the `ADMIN_TOKEN` from your `.env`
   (`(Get-Content .env | Select-String ADMIN_TOKEN)`).
5. Watch the limiter react (the first ~20 requests pass, then `429`s):

   ```powershell
   1..60 | ForEach-Object { curl.exe -s -o NUL -w "%{http_code} " http://127.0.0.1:8000/api/ping -H "X-API-Key: demo" }
   ```

Services: gateway `127.0.0.1:8000` (published), Redis `127.0.0.1:6379` (published, password required), backend and
worker are internal to the Docker network. After changing `.env`, run `docker compose up -d` again.

**No Docker?** A single-process stand-in (in-memory fakeredis) starts with `.\.venv\Scripts\python.exe -m tools.dev_stack`
(gateway `:8000`, dashboard token `dev-admin`). It is for experiments only: not the real Redis, no persistence,
meaningless performance numbers.

## 4. Run the tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q                 # all tests, in-memory Redis, about 70 s
```

Run the same tests against the **real Redis** container (uses scratch database 15 and **flushes it**; the Redis port
must be published, which `docker compose up` does):

```powershell
docker compose up -d redis
$pw = ((Get-Content .env | Where-Object { $_ -like 'REDIS_PASSWORD=*' }) -split '=',2)[1]
$env:REAL_REDIS_URL = "redis://:$pw@127.0.0.1:6379/15"
.\.venv\Scripts\python.exe -m pytest -q                 # one test is skipped in this mode (it toggles a fake server)
$env:REAL_REDIS_URL = ""
```

## 5. Traffic scenarios (Locust) and verification

Six scenarios (normal, flash crowd, sustained high, low-and-slow, randomised, multiple identities), each verified
against Redis afterwards. With the Docker stack running:

```powershell
.\load\run_scenario.ps1 -Scenario multi_identity -RunId try1     # ~3.5 minutes, new RunId every time
```

Details, predictions and recorded observations: [docs/LOAD_TESTING.md](docs/LOAD_TESTING.md). Locust only runs against
`localhost`.

## 6. Shut down

```powershell
docker compose down            # stop and remove containers (Redis data is not persisted; policy state is lost)
docker compose down --rmi local   # additionally remove the images built for this project
```

Stop the no-Docker stand-in with `Ctrl+C`. Leaving Docker Desktop running is harmless.

## 7. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `docker compose` says `Set REDIS_PASSWORD in .env` | `.env` is missing or `REDIS_PASSWORD=` is empty. Redo setup step 2 |
| `failed to connect to the docker API ... dockerDesktopLinuxEngine` | Docker Desktop is not running. Start it and wait for *Engine running* |
| Gateway keeps restarting / `healthz` not reachable | `docker compose logs gateway --tail 50`. Usually a bad value in `.env` (for example `RATE_LIMIT_FAIL_MODE` must be `open` or `closed`) |
| Right after starting Docker Desktop, `docker ps` shows Redis as `6379/tcp` instead of `127.0.0.1:6379->6379/tcp` | Docker Desktop's port forwarding was not ready. `docker compose up -d --force-recreate redis` |
| `port is already allocated` for 8000 or 6379 | Another program uses the port. Stop it, or change `REDIS_PORT` in `.env` / the gateway port mapping in `docker-compose.yml` |
| Dashboard asks for the token again / says *admin API is disabled* | `ADMIN_TOKEN` empty in `.env`, or the gateway was not recreated after editing it: `docker compose up -d` |
| Dashboard shows *Cannot refresh* and dims the numbers | The gateway or Redis is down; data shown is the last good snapshot |
| Dashboard worker card says *no heartbeat* | Worker not running: `docker compose logs worker`, `docker compose up -d worker` |
| Everything is `200`, never `429`, and `X-RateLimit-Status: bypassed` appears | Redis errors made the limiter fail open (by design, `RATE_LIMIT_FAIL_MODE=open`). Check `docker compose logs gateway` and Redis health |
| Scores stay `incomplete` | The worker has observed less than 10 s of traffic, or telemetry gaps were reported. Wait; see `docs/SCORING.md` |
| `Activate.ps1 cannot be loaded` / `run_scenario.ps1 cannot be loaded` (execution policy) | Run `powershell -ExecutionPolicy Bypass -File .\load\run_scenario.ps1 -Scenario normal` |
| `curl` in PowerShell behaves oddly | Use `curl.exe` (PowerShell aliases `curl` to `Invoke-WebRequest`) |
| `pip install` fails on a very new Python | Use Python 3.12 or 3.13 for the virtual environment |
| Tests hang or fail only in real-Redis mode | The port must be published and `REAL_REDIS_URL` must end in `/15`. See section 4 |
| Locust prints `StatsCSVFileWriter ... I/O operation on closed file` at the end | Harmless Locust shutdown message; results are unaffected |

## 8. Project structure

```
.
├── app/                         application code (runs in Docker)
│   ├── config.py                settings from environment variables (no secrets in code)
│   ├── common/                  Redis client (blocking pool), Redis key names
│   ├── gateway/                 main.py (proxy, policy + limiter), token_bucket.py (Lua), telemetry.py, policy_cache.py
│   ├── worker/                  consumer.py (Streams), engine.py (windows, EWMA), features.py (D B F C), scoring.py, params.py
│   ├── policy/                  machine.py (states, hysteresis, dwell), engine.py, params.py, store.py
│   ├── admin/                   api.py (/admin/*, /dashboard), static/dashboard.html
│   └── backend/                 demo protected API
├── tests/                       207 automated tests (+ flow_harness.py: whole system on a simulated clock)
├── load/                        Locust: definitions.py (scenarios), scenarios/*.py, observe.py (verifier), run_scenario.ps1
├── tools/dev_stack.py           no-Docker single-process stand-in
├── docs/                        ARCHITECTURE, TELEMETRY, SCORING, POLICY, LOAD_TESTING, DASHBOARD, TEST_REPORT, PHASE2_REVIEW, DEMO
├── docker-compose.yml  Dockerfile  requirements.txt  requirements-dev.txt  pytest.ini
├── .env.example  .gitignore  README.md
└── Phase1_*.pptx / .docx        the source presentation and synopsis
```

## 9. Security notes

* Secrets live only in `.env` (git-ignored). Redis requires a password; Redis and the gateway are published on
  `127.0.0.1` only; the backend is not published at all, so clients must go through the gateway.
* The admin API/dashboard is **off** unless `ADMIN_TOKEN` is set; the token is compared in constant time and is never
  accepted in a URL.
* Client identity is a hashed `X-API-Key` or the source IP; `X-Forwarded-For` is ignored. **By default any API key is
  accepted as a label** (development). For anything beyond a demo, set `ALLOWED_API_KEYS` so invented keys fall back to
  the IP identity. Risks (NAT, spoofed identities, distributed clients) are in `docs/TELEMETRY.md`.
* Not implemented: TLS, request-body size limits, real authentication of API keys, brute-force protection of the admin
  token. Do not expose this beyond localhost.

## 10. Status

See **[docs/PHASE2_REVIEW.md](docs/PHASE2_REVIEW.md)** for what is complete, partial and not done, and exactly what
has been verified by tests, by real-Redis/Docker runs, and what has not.
