# Telemetry

The gateway records one small event for every `/api/*` request and publishes it
to a Redis Stream. A separate Behaviour Worker (a later milestone) reads the
stream and computes the behavioural features. `/healthz` is not recorded.

## Path of an event

```
request handler ──record()──► in-memory queue ──► publisher task ──XADD──► Redis Stream
 (no I/O, never blocks)        (bounded, 10 000)   (batches of ≤100)       telemetry:events
```

Only the `record()` call is on the request path. If the queue is full the event is
dropped and counted; the request is never delayed or failed by telemetry.

## Redis Stream

| Item | Value |
|---|---|
| Key | `telemetry:events` (defined in `app/common/keys.py`) |
| Trimming | `XADD ... MAXLEN ~ 100000` (`TELEMETRY_STREAM_MAXLEN`). Approximate: Redis trims whole internal nodes, so the length can briefly exceed the limit. |
| Consumers | Redis consumer group (created by the worker milestone) so events are acknowledged and not re-read |
| Values | All fields are stored as strings by Redis |

## Event types

### `type=req` — one per request

| Field | Example | Meaning |
|---|---|---|
| `v` | `1` | Schema version |
| `type` | `req` | |
| `client` | `key:9f86d081884c7d65` | Client identifier (see below). Never a raw key |
| `ts` | `1761400000123` | Gateway wall-clock time (ms) when the request arrived |
| `win` | `1761400000` | One-second window id = `ts // 1000` |
| `method` | `GET` | |
| `endpoint` | `/api/items/{id}` | Path with numeric / UUID / long-hex segments replaced by `{id}`; no query string |
| `status` | `429` | Status returned to the client |
| `up_status` | `404` | Status returned by the protected API; `0` if it never answered (limited, rejected, or backend down) |
| `outcome` | `allowed` | `allowed`, `limited` (429), `blocked` (429, client in BLOCKED state), `bypassed` (Redis down, fail-open), `rejected` (Redis down, fail-closed), `bad_gateway` |
| `latency_ms` | `3` | Time spent in the gateway including the backend call |

### `type=tick` — gateway heartbeat (no data)

Emitted by each gateway every `TELEMETRY_TICK_S` seconds (default 1; 0 disables): `{v, type=tick, ts, win}`. It tells the worker
"this gateway and its telemetry path are alive, and no requests happened in this second", so time can advance while the system is idle
and clients that stopped can recover. Ticks are not request telemetry: they are excluded from request counts, from
`published`/`lost` counters and from gap reports, and the dashboard ignores them.

### `type=gap` — reported data loss

| Field | Meaning |
|---|---|
| `v`, `type=gap` | |
| `ts` | When the gap was reported |
| `lost` | Number of request events that were lost |
| `from_ts`, `to_ts` | Earliest and latest `ts` among the lost events |

## How the behavioural inputs are derived (done by the worker, next milestone)

| Input | Derived from events |
|---|---|
| Request rate | count of `req` events per `client` per `win` (includes `limited` ones: they are real demand) |
| Peak burst | the largest one-second count for the client within the observation period |
| Upstream 4xx failure rate | events with `400 <= up_status < 500` divided by events in the window. `limited` events have `up_status=0` so the gateway's own 429s can never raise F |
| Endpoint concentration | share of the client's events that hit its most-used `endpoint` |

## Telemetry failures must not look like quiet clients

A missing event looks exactly like "the client sent fewer requests", which would
lower the risk score and let an attacker through. Therefore:

* Events dropped because the queue is full, and events that could not be written
  because Redis was down, are **counted** (`/healthz` → `telemetry.lost`).
* When Redis is reachable again the publisher writes a `gap` event covering the lost
  events. **Worker rule (to be implemented): any window overlapping `[from_ts, to_ts]`
  is "incomplete"; it must not lower a risk score, update an EWMA baseline, or
  trigger recovery.**
* Limits that remain: if the *gateway process itself* crashes, queued events and the
  unreported gap counter are lost with it (no gap event is possible). Stream trimming
  can also delete events the worker has not read yet if the worker falls far behind.
  Both are documented limitations, not detected automatically.
* Timestamps come from each gateway's own clock; several gateways need synchronised
  clocks (NTP). Redis stream IDs (server time of insertion) are available as a fallback.

## Privacy

Recorded: the fields above only. **Never recorded:** request/response bodies, query
strings, `Authorization`, `Cookie`, `X-API-Key` or any other header, raw API keys.
This is verified by `tests/test_telemetry.py::test_no_secrets_in_telemetry`.

## How the client identifier is obtained, and why it is a risk

1. `X-API-Key` header → `key:` + first 16 hex chars of SHA-256 of the key.
2. Otherwise the TCP source address → `ip:<address>`.
3. `X-Forwarded-For` is **ignored** (clients can forge it).

Risks of trusting a client-supplied identifier:

* **Evasion by rotation:** if any string is accepted as an API key, an attacker sends a
  new random key per request and gets a fresh full token bucket and a fresh "clean"
  behavioural history every time.
  *Mitigation implemented:* set `ALLOWED_API_KEYS` (comma separated). Unknown keys fall
  back to the IP identity, so inventing keys gains nothing. Without this setting
  (default, development only) any key is accepted.
* **Framing / score poisoning:** if keys are guessable or leaked, someone else can send
  abusive traffic under a victim's key and push the victim into THROTTLED/BLOCKED.
  Keys must be secret and long; real deployments should authenticate the key
  (signature or lookup) before using it as an identity.
* **Shared IPs (NAT, offices, mobile carriers):** many users behind one address share
  one bucket and one risk score; one abuser can penalise the rest.
* **IP rotation / distributed clients:** an attacker with many addresses gets many
  identities. The synopsis lists "distributed clients" as a Phase III scenario; this
  design does not claim to solve it.
* **Behind a reverse proxy:** every request would appear to come from the proxy's
  address. Trusting `X-Forwarded-For` is only safe when set by a proxy you control.
* **Hash truncation:** 16 hex chars (64 bits) is for readability and avoids storing raw
  keys; collisions are astronomically unlikely and not a security boundary.
