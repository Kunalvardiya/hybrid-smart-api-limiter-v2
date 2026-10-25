# Phase II completion review

Date: 25 Oct 2026. Reviewed against the synopsis (`Phase1_Report_Hybrid_Smart_API_Limiter.docx`) and the presentation
(`Phase1_Adaptive_API_Shield_Final.pptx`). Phase III (full experimental comparison against a static limiter, final
evaluation, final report) is out of scope and is **not** claimed anywhere below.

**Verdict.** All Phase II functionality named in the documents is implemented, and the core flow has been verified by
207 automated tests (206 + 1 skipped on a real Redis) and by runs on the real Docker stack. What is **not** verified is
listed explicitly in section 5. Nothing is committed to Git yet (awaiting your approval).

Evidence levels used below: **T** = automated tests, **R** = tests run against real Redis, **D** = exercised on the Docker
stack (real containers, Locust), **B** = looked at in a browser, **—** = not verified.

## 1. Requirements in the documents → implementation

| # | Requirement (source) | Status | Where | Evidence |
|---|---|---|---|---|
| 1 | Hybrid architecture: deterministic token bucket + behavioural risk assessment (Objective 1) | Complete | `docs/ARCHITECTURE.md` | T R D |
| 2 | Main path synchronous and light: gateway with **local policy cache** → token bucket → API | Complete | `app/gateway/` | T R D |
| 3 | Asynchronous path: telemetry → **Redis Stream** → Behaviour Worker → Risk/Policy Engine → **Redis Policy Store** → fed back to the gateway | Complete | `telemetry.py`, `worker/`, `policy/` | T R D |
| 4 | Observe traffic in **1-second windows**: request rate, peak burst, upstream 4xx rate, endpoint concentration | Complete | `worker/engine.py` | T R D |
| 5 | **EWMA** baseline during normal behaviour | Complete (guards documented) | `worker/features.py`, `engine.py` | T D |
| 6 | Features **D, B, F, C** normalised to [0, 1] | Complete; exact curves are implementation choices | `docs/SCORING.md` | T D |
| 7 | **Risk = 100 × (0.25D + 0.25B + 0.25F + 0.25C)** | Complete, fixed weights | `worker/scoring.py` | T (and recomputed from stored values in 6 integration scenarios) R D |
| 8 | Compare score with thresholds; states **NORMAL → MONITORED → THROTTLED → BLOCKED** | Complete; threshold values are implementation choices (25/50/75) | `policy/machine.py` | T R D |
| 9 | **Hysteresis** and **minimum dwell time** | Complete (5 points; 10/30/60 s) | `policy/params.py` | T D |
| 10 | **Recovery**: access progressively restored when abnormal behaviour stops | Complete: one level at a time; works for an idle system (heartbeat) | `policy/machine.py`, `telemetry.py` | T D |
| 11 | Interpretable / explainable risk score and decisions (Objective 2, expected outcome) | Complete | stored features + reasons, dashboard | T B D |
| 12 | Redis, Docker, Python framework, Git/GitHub, Locust, API testing tool, browser dashboard (software list) | Mostly: Git repository initialised, **no commit and no GitHub remote yet**; `curl` serves as the API testing tool | — | — |
| 13 | Phase II planned activities: gateway, token bucket, telemetry, behavioural scoring | Complete | | |
| 14 | Evaluation scenarios (normal, flash crowd, sustained, low-and-slow, randomised, distributed) | Traffic generators exist for Phase II demonstration; **the comparison/evaluation itself is Phase III** | `load/` | D (one scenario), stand-in (all six) |
| 15 | Evaluation of security, fairness, performance and overhead against a static limiter (Objective 4) | **Not started — Phase III** | | |

## 2. Review of the 17 requested areas

| Area | Status | Verified by | Limitations / notes |
|---|---|---|---|
| 1. API gateway | Complete | T R D | No TLS, no request-body limit, `HEAD`/`OPTIONS` not proxied (405), no streaming/WebSocket |
| 2. Deterministic token bucket | Complete | T R D | Atomic Lua on the Redis clock; global capacity/refill (per-client limits only via the policy multiplier) |
| 3. Request telemetry | Complete | T R D | Gateway crash loses queued events and cannot report them; stream trimming can drop unread events (both documented) |
| 4. Asynchronous Redis Streams | Complete | T R D | Single worker; at-most-once after ack (restart yields `incomplete`, never a guess) |
| 5. EWMA baselines | Complete | T D | Clients abusive from their first request never get a baseline (documented) |
| 6. D, B, F, C | Complete | T D | B is relative to the limiter's allowance, not to the client's own history (deliberate; documented) |
| 7. Risk formula | Complete | T R D | |
| 8. Progressive enforcement states | Complete | T R D | |
| 9. Hysteresis and dwell | Complete | T D | Values unvalidated against real attack data (Phase III) |
| 10. Recovery | Complete | T D | Verified on Docker: flooder left BLOCKED 8 s and was fully NORMAL 48 s after its flood ended (it had been blocked ≥ 60 s); scrapers recovered on an idle system |
| 11. Redis policy storage | Complete | T R D | Redis runs without persistence: a Redis restart loses baselines and policy state |
| 12. Automated tests | Complete for Phase II scope | 207 pass; 206 + 1 skipped on real Redis | Gaps listed in section 5 |
| 13. Locust scenarios | Complete (6 scenarios + verifier) | stand-in: all six; Docker: `multi_identity` | The other five not yet run on Docker; one prediction was refuted and revised (recorded) |
| 14. Dashboard | Complete | T B D | Embedded browser only; polls every 2 s; request figures are windowed, not lifetime |
| 15. Docker Compose | Complete | D R | Verified: build, start, health, restart, tests, scenario. Not verified: other machine, `docker compose down` after a long run is trivial but untested here |
| 16. Security and configuration | Partial | T | Secrets only in `.env`; Redis password mandatory; ports on `127.0.0.1`; backend unpublished; admin token constant-time. **Any API key is accepted by default**; no TLS; no key authentication; no admin brute-force protection |
| 17. README and setup | Complete | partially | Steps executed piece by piece (fresh virtualenv install + tests passed; `.env` snippet checked); not run on a different computer |

## 3. Defects found and fixed in this review

Details and regression tests: `docs/TEST_REPORT.md`.

1. Redis connection pool exhausted by bursts → limiter failed open under load (found only on real Redis).
2. Compressed upstream responses mislabelled.
3. Dashboard queries tripped the strict request-path timeout (own client, `hiredis`, smaller default window).
4. A restricted client that never built a baseline could never be released after going silent.
5. An idle system never advanced time, so recovery needed fresh traffic (heartbeat `tick` events added).
6. Backend demo API was reachable from the host, bypassing the gateway.
7. `.env.example` shipped a guessable password.

## 4. Deliberate design choices and known limitations (not defects)

* A client with **no history** cannot exceed THROTTLED through volume and concentration (D needs a baseline).
* THROTTLED barely affects **very slow bots** (the throttled bucket is still above their rate), and they cannot reach BLOCKED.
* **Low-and-slow** traffic below ~10 requests per 10 s is mostly invisible (F and C need minimum samples); sometimes flagged by chance.
* **Distributed** clients are scored independently; nothing correlates identities.
* Single-endpoint legitimate clients carry +25 points from C (MONITORED boundary).
* Identity is a label (API key hash or IP), not authenticated; NAT-shared IPs share one state.
* All thresholds, windows and smoothing values are **implementation choices** because the documents give none.
  They are configurable and documented but **unvalidated**.
* Fixed 0.25 weights, as in the documents; correlation-based or learned weights were discussed and deliberately not implemented.

## 5. Not yet verified (be honest about these)

* The other five Locust scenarios on the Docker stack.
* Several gateway instances on one Redis; clock skew.
* Worker crash mid-batch; Redis restart; Redis down for a long time under load on Docker.
* Throughput, latency, CPU and memory overhead (Phase III), including any claim that the fast path is "low latency".
* Setup on a clean second machine; browsers other than the embedded one.
* Threshold and parameter quality against real attack data.

## 6. Phase II checklist

### Completed
- [x] API gateway (client identification, policy lookup, proxy)
- [x] Deterministic Redis token bucket (atomic Lua), headers, 429, configurable limits, failure modes
- [x] Request telemetry (schema, 1-second windows, privacy, loss/gap reporting, heartbeat)
- [x] Asynchronous Redis Streams processing (consumer group, acks, restart recovery)
- [x] EWMA behavioural baselines
- [x] Normalised features D, B, F, C
- [x] Specified risk-score formula
- [x] Enforcement states NORMAL → MONITORED → THROTTLED → BLOCKED
- [x] Hysteresis and minimum dwell time
- [x] Recovery (gradual; works on an idle system)
- [x] Redis policy storage and the gateway's local policy cache
- [x] Automated tests (207)
- [x] Locust scenarios (6) with a Redis-side verifier
- [x] Monitoring dashboard with loading/empty/error states
- [x] Docker Compose setup
- [x] Documentation: architecture, telemetry, scoring, policy, load testing, dashboard, test report, demo guide, README

### Partially completed
- [~] Security: sound defaults, but any API key is accepted unless `ALLOWED_API_KEYS` is set; no TLS/authentication/body limits
- [~] Software list: Git repository exists but nothing is committed and there is no GitHub remote (awaiting your approval)
- [~] Locust scenarios on Docker: 1 of 6 run there
- [~] Redis durability: no persistence (policy state is lost on a Redis restart)
- [~] Reproducible setup: verified piecemeal on this machine, not on a second one

### Not completed (out of Phase II scope or not started)
- [ ] Comparison against a static limiter, security/fairness/performance/overhead evaluation, final report (**Phase III**)
- [ ] Threshold/parameter calibration (**Phase III**)
- [ ] Correlation-based or learned weights (not required; documented decision to keep 0.25)
- [ ] Authentication of API keys, TLS, multi-gateway deployment tests

### Verified by automated tests (207; 206 + 1 skipped on real Redis)
- [x] Token bucket atomicity, refill, capacity, concurrency (100 simultaneous requests → exactly the capacity)
- [x] Telemetry content, windows, privacy (no secrets recorded), loss reporting, ticks
- [x] Scoring maths, normalisation, EWMA, edge cases, the formula
- [x] Missing/insufficient telemetry never produces a misleading score
- [x] State machine: upward/downward moves, boundaries, hysteresis, dwell, recovery, restart restore
- [x] Gateway applies THROTTLED/BLOCKED, policy staleness, Redis failure modes
- [x] Whole flow: legitimate, burst, repeated 4xx, concentrated endpoint, concurrency, errors, non-blocking
- [x] Dashboard API: access control, counts equal to what was sent, failures, privacy

### Verified on the real Docker stack (not just by tests)
- [x] Build, start, health checks, backend unpublished
- [x] Full test suite against the real Redis container
- [x] `multi_identity` scenario twice: telemetry 100 %, no limiter bypass, scoring, states, recovery
- [x] Dashboard figures equal Locust's counters exactly; page loads with live data

### Not yet verified
- [ ] Docker runs of the other five scenarios
- [ ] Multi-gateway, worker crash mid-batch, Redis restart / long outage under load
- [ ] Performance and overhead numbers (Phase III)
- [ ] Clean-machine setup, other browsers
- [ ] Parameter/threshold quality
