# Monitoring dashboard

A single static page served by the gateway at **`/dashboard`**, backed by three read-only JSON endpoints
under **`/admin`**. No framework, no build step, no external scripts or fonts (it works offline).
It refreshes every 2 seconds.

## Run and open it

1. Pick an admin token and give it to the gateway. The admin API is **disabled until `ADMIN_TOKEN` is set**
   (secure default); without it the page loads but shows "admin API is disabled".

   **Docker stack:** put a long random value in `.env` (git-ignored), then start as usual:

   ```bash
   # .env  ->  ADMIN_TOKEN=<long random value>
   docker compose up --build -d
   ```

   **No-Docker stand-in:** `python -m tools.dev_stack` sets the token `dev-admin` for you and prints it.

2. Open <http://127.0.0.1:8000/dashboard>, paste the token, press *Open dashboard*.
   The token lives in this tab's `sessionStorage` and is sent as an `X-Admin-Token` header; "Forget token" removes it.
3. Generate some traffic (see `docs/LOAD_TESTING.md`, e.g. `.\load\run_scenario.ps1 -Scenario multi_identity ...`)
   and watch the page react.

The gateway binds to `127.0.0.1` in Docker Compose. Client ids contain IP addresses and hashed API-key labels;
do not expose `/admin` or `/dashboard` beyond your machine.

## What each panel shows, and where the number comes from

Nothing is estimated or made up. If the backend has no value the page shows "—" or an explicit empty message.

| Panel / figure | Source in Redis | Notes |
|---|---|---|
| Redis OK/DOWN | the API's own Redis calls | If Redis is unreachable the API returns 503 and the page dims its old numbers and shows an error banner |
| Worker running / stale / no heartbeat | `worker:heartbeat` (new: the worker writes it about once a second, expires after 30 s) | *running* < 5 s old, *stale* otherwise, *no heartbeat* if absent (not started, or dead for 30 s) |
| Stream length, lag, unacknowledged | `XLEN`, `XINFO GROUPS` on `telemetry:events` | *lag* is shown as "—" when Redis does not report it |
| Telemetry events lost since gateway start | gateway publisher counter | events dropped by the gateway itself (see `docs/TELEMETRY.md`) |
| Policy cache age | gateway policy cache | how long since the gateway last refreshed the policy table |
| Total / allowed / 429 / BLOCKED requests | `telemetry:events`, last *N* seconds | `allowed` = forwarded (includes fail-open bypasses, listed separately when present) |
| Upstream 4xx rate | same events | 4xx answers from the protected API ÷ requests that reached it. The gateway's own 429s are excluded. "—" when nothing reached the API |
| Avg requests/s, busiest second, per-second chart | same events, grouped by their one-second window id | avg is over the seconds actually covered, not the full window |
| Active clients | distinct client ids in the window | |
| Clients MONITORED / THROTTLED / BLOCKED | `policy:enforced` | right now, not windowed |
| Clients table | `policy:{client}` | latest worker record: state, score, D, B, F, C, rate, peak per second, baseline, request / 4xx counts over the scoring span, top endpoint, unavailable features. Score is "—" and the status shows *incomplete* when the worker declined to score |
| Policy updates | `policy:events` | every state change with the score, D/B/F/C and the reason |

**Known limits (also stated on the page):**
* Request totals cover only the chosen window (30 s, 60 s or 5 min) and are read from the **capped** telemetry stream.
  They are not lifetime totals; lifetime counters would add Redis writes to the request path, so none are kept.
* If the window holds more than 5 000 events (`cap`, default 5 000) it is cut to the newest 5 000 and the page says totals are lower bounds.
  The same applies when the system has reported telemetry gaps.
* Client rows are the worker's latest records, so a client that went quiet keeps its last record until it returns.
* At most the 50 highest-risk clients and 30 latest policy updates are shown.
* The page polls; there is no push. A restricted client may appear up to ~2 s after the state changed.

## API (all `GET`, all need the `X-Admin-Token` header)

| Endpoint | Query | Returns |
|---|---|---|
| `/admin/overview` | `seconds` (5-600, default 60), `cap` (100-100 000, default 5 000) | `health` (redis, worker, stream, gateway counters), `window` (totals, outcomes, upstream failure rate, rps, peak, per-second series, gap info), `enforced` (state counts) |
| `/admin/clients` | `limit` (1-200, default 50) | `total_clients`, `returned`, `clients[]` sorted by state then score |
| `/admin/events` | `limit` (1-200, default 50) | `events[]` newest first |

Responses: `401` wrong/missing token (tokens in the URL are not accepted), `503` admin disabled or Redis unavailable,
`422` bad query parameter.

```bash
curl -H "X-Admin-Token: <token>" http://127.0.0.1:8000/admin/overview
```

## Verified

* 21 automated tests in `tests/test_admin.py`: access control, empty system, counts equal to requests actually sent,
  window truncation and gap reporting, worker heartbeat (fresh/stale), the contract between the worker's record format
  and the clients endpoint, ordering/limits, Redis failure, raw API keys never appearing, page served with no external
  resources.
* Live check (no-Docker stand-in, real browser, `multi_identity` scenario): login with a wrong and a correct token,
  live updates showing the flooder being throttled and blocked, the empty state on a fresh system, and the error
  state when the gateway was stopped (old numbers dimmed, banner shown). After the run the API reported
  **8 167 requests, 2 705 throttled/blocked and 2 224 upstream 4xx**, identical to Locust's own counters.
* Docker stack (real Redis): after a 8 117-request scenario the API reported exactly Locust's counters
  (8 117 requests, 2 786 throttled/blocked, 2 234 upstream 4xx); the page loaded and showed live data; a 600-second,
  8 117-event query took 87 ms with hiredis in the image. Monitoring reads use their own Redis client with a 5 s
  timeout, so they cannot trip the request path's 0.5 s limit.
* Not verified: browsers other than the embedded one, and very large client lists (> 200).
