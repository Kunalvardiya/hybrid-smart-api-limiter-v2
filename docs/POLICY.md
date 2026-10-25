# Adaptive policy and progressive enforcement

## Source-defined vs implementation choices

| Defined by the synopsis / presentation | Chosen by this implementation (configurable) |
|---|---|
| Four states **NORMAL → MONITORED → THROTTLED → BLOCKED** | What each state does to a client (see table below) |
| States selected by **risk thresholds, hysteresis and a minimum dwell time** | **Every number**: thresholds, hysteresis width, dwell times, throttle strength |
| On recovery, access is **progressively restored** | "Progressive" = one level at a time, each with its own dwell time |
| The deterministic token bucket stays as the baseline protection | How the policy scales it (multiplier), and the fallback when policy is unavailable |

The documents give **no numeric values**. All numbers below are implementation choices;
justify or tune them in Phase III.

## How a risk score affects a client

| State | Entered when score ≥ | Left (downward) when score < | Minimum time before it may be left | Effect on the client |
|---|---|---|---|---|
| NORMAL | — | — | — | Full token bucket |
| MONITORED | **25** | 20 | 10 s | Full bucket. Flagged (`X-Policy-State` header, `policy:events`) so behaviour can be watched |
| THROTTLED | **50** | 45 | 30 s | Bucket capacity **and** refill rate × **0.25** (min capacity 1). Excess gets `429` + `Retry-After` |
| BLOCKED | **75** | 70 | 60 s | Every request gets `429` + `Retry-After: 60` at once; the token bucket is not touched |

Why 25 / 50 / 75: D, B, F and C each add at most 25 points, so a threshold counts how many
signals are fully saturated: one → MONITORED, two → THROTTLED, three → BLOCKED. Example:
a flood spread over several endpoints saturates D and B (score 50 → THROTTLED); the same
flood aimed at one endpoint also saturates C (score 75 → BLOCKED).

Blocked and throttled requests are still written to telemetry (`outcome=blocked|limited`,
`up_status=0`). They count as demand, so a client that keeps hammering keeps its score high.
They never count as upstream failures, so F cannot be inflated by enforcement itself.

## Transition rules (`app/policy/machine.py`, pure and deterministic)

* **Up:** as soon as the score reaches a higher state's entry threshold (`>=`, inclusive),
  move there, even several levels at once. No dwell time applies to escalation.
* **Down:** only **one level at a time**, and only if **all** hold:
  1. score is **strictly below** the state's exit threshold (entry − hysteresis);
  2. the client has spent at least the state's dwell time in it;
  3. the record is fully informed (`status=scored`). A `warming_up` score is only a lower bound.
  Each step down restarts the dwell timer.
* **Hold:** otherwise, including a score inside the hysteresis band and any `incomplete`
  record (cold start, worker restart, telemetry gap). Missing data never releases or
  escalates a client.
* A client at score 26 then 24 then 26 … enters MONITORED once and stays; it does not flap.

**Recovery timeline** (abuse stops, score falls to 0 and stays there, defaults):
BLOCKED → THROTTLED after 60 s → MONITORED after a further 30 s → NORMAL after a further
10 s: **100 s** in total. A silent client keeps being evaluated while it is not NORMAL, so
recovery does not depend on the client sending traffic. If abuse resumes mid-recovery the
client escalates again immediately.

Time is event time (the one-second window ids in telemetry), the same clock the scorer uses.
Per evaluation cycle a client moves down at most one level.

## How the gateway gets and applies policy

```
worker ──HSET──► policy:enforced  "BLOCKED:0:1761400000123"    (only non-NORMAL clients)
                      │  copied every POLICY_REFRESH_S (1 s) by a background task
                      ▼
gateway local dict ──get(client)──► O(1) lookup in the request path (no Redis call)
```

* The request path only reads a dictionary. Policy refresh runs in a separate task and swaps
  in the new table with one assignment, so updates never block requests.
* A new policy reaches the gateway within ~`POLICY_REFRESH_S` seconds (default 1 s).
* **BLOCKED:** immediate `429`, no Redis call at all.
* **THROTTLED:** the normal Lua token bucket is called with multiplier 0.25. The Lua script
  caps stored tokens at the new, smaller capacity, so the change takes effect on the next
  request and recovery is smooth when the multiplier rises again.
* **Worker down or Redis unreachable (fail toward the plain token bucket):**
  entries older than `POLICY_MAX_AGE_S` (120 s) are ignored because the worker refreshes them
  on every evaluation; if the table cannot be fetched for that long the whole cache is
  ignored. Within that time the last known policy keeps being enforced.
  Entries are refreshed on every worker evaluation, and the gateway's one-second heartbeat events keep the worker evaluating even when there is no traffic, so a quiet system does not make live entries stale. If the gateway/telemetry path stops, nothing advances and nothing is released (by design).

## Redis keys

| Key | Type | Content |
|---|---|---|
| `policy:enforced` | hash | client → `STATE:multiplier:updated_ms` for non-NORMAL clients. **Read by the gateway** |
| `policy:{client}` | hash | latest record: score, D, B, F, C, baseline, `status`, `state`, `state_since`, `multiplier`, `state_reason` (latest evaluation), `last_transition` (why the state last changed) |
| `policy:events` | stream | one entry per state change: client, from, to, window, score, D/B/F/C, reason |
| `risk:log` | stream | history of score records |

After a worker restart, enforcement is restored from `policy:enforced` / `policy:{client}`.

## Configuration

| Variable | Default | |
|---|---|---|
| `POLICY_ENTER_MONITORED` / `_THROTTLED` / `_BLOCKED` | 25 / 50 / 75 | entry thresholds (must increase) |
| `POLICY_HYSTERESIS` | 5 | must be smaller than the gap between thresholds |
| `POLICY_DWELL_MONITORED_S` / `_THROTTLED_S` / `_BLOCKED_S` | 10 / 30 / 60 | minimum time before leaving |
| `POLICY_THROTTLE_MULTIPLIER` | 0.25 | THROTTLED share of capacity and refill rate |
| `POLICY_REFRESH_S` | 1 | gateway policy refresh interval |
| `POLICY_MAX_AGE_S` | 120 | policy older than this is ignored |

Invalid combinations (for example hysteresis wider than a threshold gap) stop startup.

## Known limitations

* **Enforcement hides evidence.** A BLOCKED client has no served requests, so F becomes
  unavailable and drops out of its score. A persistent abuser may therefore fall from BLOCKED
  to THROTTLED after the dwell time and be re-escalated if its behaviour still warrants it.
* **Single-endpoint clients** carry +25 from C, which alone puts them at the MONITORED boundary.
* **Shared identities (NAT)** share one state; see `docs/TELEMETRY.md`.
* **One level per cycle.** After a long backlog the worker catches up one step per batch.
* **Thresholds are unvalidated defaults.** Calibrating them against the evaluation scenarios
  is Phase III work.
