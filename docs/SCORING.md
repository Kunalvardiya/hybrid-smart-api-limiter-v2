# Behavioural scoring

## What comes from the source documents, and what does not

| Fixed by the synopsis / presentation | Chosen by this implementation (configurable) |
|---|---|
| Observe traffic in **1-second windows** | How many windows a score looks at (`span_s`) |
| Signals: request rate, peak burst, upstream 4xx failure rate, endpoint concentration | The exact normalisation curve of each feature |
| **EWMA baseline** during normal behaviour | Smoothing factor, warm-up length, what counts as "normal" |
| Features **D, B, F, C** each scaled to **[0, 1]** | All numeric reference values below |
| **Risk = 100 × (0.25 D + 0.25 B + 0.25 F + 0.25 C)** | — |
| States, thresholds, hysteresis, dwell time | *Not in this milestone.* Scores are produced; no enforcement state is assigned yet |

Everything in the right-hand column is an **implementation choice, not a result from the
source documents**. Change it with environment variables (`.env`), and state the values you
used in the Phase III report.

## Pipeline

```
telemetry:events ──XREADGROUP──► BehaviourEngine ──► policy:{client} (hash, latest)
 (req + gap events)               1-s windows           risk:log (stream, history)
                                  span of last N s
                                  D, B, F, C → score
```

Every second that closes, for every active client, the engine looks at the last `span_s`
windows (default 10) and computes:

| Quantity | Meaning |
|---|---|
| `rate` | requests in the span ÷ `span_s` (includes requests the limiter rejected: they are demand) |
| `peak` | the busiest single second in the span |
| `n_served` | requests that reached the protected API (`up_status > 0`) |
| `n_4xx` | of those, how many the protected API answered with 400–499 |
| `top_share` | share of the span's requests that hit the single most-used endpoint |

## The four features (all in [0, 1])

| Feature | Formula | Default parameters | Reads as |
|---|---|---|---|
| **D** rate deviation | `clip01((rate − baseline) / (d_saturation × max(baseline, rate_floor)))` | `d_saturation=2`, `rate_floor=1` req/s | 0 at or below the client's own baseline; 1 at 3× baseline |
| **B** burst intensity | `clip01((peak − burst_ref) / burst_ref)` | `burst_ref=10` req/s (defaults to `BUCKET_REFILL_PER_SEC`) | 0 if the busiest second stays within the sustained allowance; 1 at 2× it |
| **F** failure rate | `n_4xx / n_served` | needs `n_served ≥ min_requests_f=5` | share of served requests the API rejected with 4xx |
| **C** endpoint concentration | `clip01((top_share − c_floor) / (1 − c_floor))` | `c_floor=0.5`, needs `≥ min_requests_c=10` requests | 0 if no endpoint gets more than half the traffic; 1 if all of it goes to one |

`clip01(x)` forces a value into [0, 1]. Design notes:

* **F ignores the gateway's own 429s** (they have `up_status=0`), both in the numerator and the
  denominator. Being throttled can neither raise F nor dilute it. (The telemetry document first
  proposed dividing by all events; dividing by *served* requests is the better definition and
  is what is implemented.)
* **B is relative to the limiter's allowance, not to the client's baseline,** so an attacker
  who is present from the first request cannot make a flood look normal by "training" the
  baseline.
* **C alone cannot be 1 for a quiet client:** it needs `min_requests_c` requests in the span.
  A normal client that really uses a single endpoint will carry +25 points from C; that is a
  known limitation of this definition and a reason thresholds must be tuned in Phase III.

## EWMA baseline

`baseline ← alpha × rate + (1 − alpha) × baseline` with `alpha = 0.02` (one update per second,
so the baseline remembers roughly the last minute).

The baseline is **only updated when** the span has traffic, observation is complete, and the
evaluation's score is below `baseline_update_max_risk` (default **20**; a single fully
saturated feature already gives 25). It therefore does not learn from abuse, and idle time
does not drag it toward 0. `D` is **unavailable** until the baseline has learned from
`warmup_evals` (default 30) normal evaluations.

Known limitation: a client that is abusive from its very first request never gets a
baseline (D stays unavailable); B, F and C still score it.

## Missing or insufficient telemetry never produces a misleading score

| Situation | What the worker does |
|---|---|
| Worker just started / first span not yet fully observed | `status=incomplete`, **no score** |
| Span overlaps a `gap` event (events were lost) | `status=incomplete`, **no score**, baseline untouched |
| Stream goes silent (nothing arrives) | Windows only close when newer events arrive, so silence caused by a *broken telemetry path* is never evaluated as "quiet". While the gateway is alive it emits a heartbeat `tick` event every second (`TELEMETRY_TICK_S`), which moves time forward, so a genuinely idle system lets restricted clients recover. If the gateway or Redis is down there are no ticks and nothing is released |
| No requests in the span | D is 0 for any baseline, so the score is complete (`status=scored`) even without a baseline. This lets a silent client be released; before this rule a restricted client that never built a baseline could stay stuck forever |
| Fewer than `min_requests_f` served requests / `min_requests_c` requests | F or C is **unavailable**: counted as 0, listed in `unavailable`, explained in `notes` |
| D not yet available (warm-up) | `status=warming_up`, `D` listed in `unavailable`. The score is then a *lower bound*: it can understate risk but never overstate it |
| Event arrives after its window was finalised (> `grace_s` late) | Counted in `late_events`, not applied |
| Malformed event | Counted in `bad_events`, skipped |

Consumers of the policy data (the enforcement milestone) must check `status` and `scored_win`
before trusting `score`: an `incomplete` record keeps the previous score fields so the history
is not erased, but marks them stale.

## What is stored in Redis

`policy:{client}` (hash, overwritten each evaluation):

| Field | Example | Notes |
|---|---|---|
| `status` | `scored` | `scored`, `warming_up` or `incomplete` |
| `score` | `65.00` | absent on records that never had one |
| `scored_win` | `2054` | window of the last real score |
| `win` | `2060` | window of this record |
| `D` `B` `F` `C` | `0.7000` | the four features, 4 decimals |
| `rate`, `peak` | `12.000`, `25` | |
| `baseline`, `obs_count` | `5.0000`, `64` | persisted so a worker restart keeps the baseline |
| `n_requests`, `n_served`, `n_4xx` | | counts behind F and C |
| `top_endpoint`, `top_share` | `/api/a`, `0.8000` | |
| `unavailable` | `F,C` | features that had too little data |
| `notes` | text | human-readable reasons |

`risk:log` (stream, capped ~50 000): the same fields plus `client`, for clients with traffic.

## Worked examples (these numbers are asserted by the tests)

**Example 1 — one evaluation, by hand.** A client has `baseline = 5` req/s. In the last
10 s it sent 120 requests (`rate = 12`), its busiest second had 25, 36 of the 120 answers
were 404, and 80 % of requests hit one endpoint.

| | Calculation | Value | Points (25 × value) |
|---|---|---|---|
| D | (12 − 5) / (2 × 5) | 0.70 | 17.5 |
| B | (25 − 10) / 10 = 1.5 → clipped | 1.00 | 25.0 |
| F | 36 / 120 | 0.30 | 7.5 |
| C | (0.8 − 0.5) / 0.5 | 0.60 | 15.0 |
| **Risk** | 100 × (0.25·0.7 + 0.25·1 + 0.25·0.3 + 0.25·0.6) | | **65.0** |

**Example 2 — scenarios simulated with the engine** (defaults, 4 endpoints used evenly
unless stated):

| Scenario | D | B | F | C | Score |
|---|---|---|---|---|---|
| 5 req/s for 2 minutes, all 200 | 0 | 0 | 0 | 0 | **0** (baseline → 5.0) |
| then 50 req/s | 1 | 1 | 0 | 0 | **50** (baseline stays 5.0: no poisoning) |
| 10 req/s, every response 404, 10 different endpoints | n/a (warming up) | 0 | 1 | 0 | **25** (`status=warming_up`) |
| 10 req/s, all to one endpoint, all 200 | n/a | 0 | 0 | 1 | **25** |
| 20 req/s all rejected by the limiter (`up_status=0`) | n/a | 1 | n/a | 0 | **25**: demand is visible, F honestly unavailable |
| 1 request in the span | n/a | 0 | n/a | n/a | **0**, with `F,C` listed as unavailable |

## Parameters (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `SCORING_SPAN_S` | 10 | windows per evaluation |
| `SCORING_ALPHA` | 0.02 | EWMA smoothing factor |
| `SCORING_D_SATURATION` | 2.0 | D = 1 at (1 + this) × baseline |
| `SCORING_RATE_FLOOR` | 1.0 | minimum divisor (req/s) in D |
| `SCORING_BURST_REF` | `BUCKET_REFILL_PER_SEC` (10) | B = 0 at this peak, 1 at twice it |
| `SCORING_C_FLOOR` | 0.5 | top-endpoint share that gives C = 0 |
| `SCORING_MIN_REQUESTS_C` / `_F` | 10 / 5 | evidence needed before C / F count |
| `SCORING_WARMUP_EVALS` | 30 | normal evaluations before D is used |
| `SCORING_BASELINE_MAX_RISK` | 20 | baseline learns only below this score |
| `SCORING_GRACE_S` | 2 | seconds a window waits for late events |

## Running it

```bash
docker compose up --build -d          # gateway + backend + redis + worker
```

Inspect the result for a client (the key shown in the stream or `/healthz` flow):

```bash
docker compose exec redis sh -c 'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning HGETALL "policy:ip:172.18.0.1"'
```

Without Docker, with a local Redis: `python -m app.worker`.
