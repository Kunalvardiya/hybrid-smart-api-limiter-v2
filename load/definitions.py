"""Declarative traffic scenarios (Phase II demonstration only; not the Phase III evaluation).

A scenario is a set of ROLES. Each role has a number of Locust users; every user is one client
identity (an `X-API-Key` such as `lt-flash-r1-crowd-07`) that follows a list of PHASES
(duration, request rate, endpoint pattern). Everything is reproducible: identities are
deterministic, and randomness comes from a per-identity RNG seeded with SCN_SEED + identity.

Environment variables (all optional)
  SCN_RUN_ID      suffix that makes identities unique per run (default r1). Use a NEW id for every
                  run: baselines and policy state are kept in Redis per identity.
  SCN_SEED        RNG seed (default 1)
  SCN_TIME_SCALE  multiplies every phase duration (default 1.0)
  SCN_RATE_SCALE  multiplies every request rate (default 1.0)
  SCN_COUNT_<ROLE> overrides the number of users of a role, e.g. SCN_COUNT_CROWD=10
  MAX_USER_RATE   hard cap on requests/second of a single user (default 100)

`expect` lists what the DESIGN predicts for each role. The observer (load/observe.py) checks the
prediction against what Redis really recorded; a prediction is a hypothesis, not a result.
"""
import os
import random
from dataclasses import dataclass, field
from typing import Callable

STATE_ORDER = {"NORMAL": 0, "MONITORED": 1, "THROTTLED": 2, "BLOCKED": 3}

# endpoint patterns: "mixed", "single:<path>", "scan404", "fail401"
MIXED = [("/api/ping", 0.30), ("/api/items/{n}", 0.25), ("/api/users/{n}", 0.20),
         ("/api/orders", 0.15), ("/api/search?q=x", 0.10)]


@dataclass(frozen=True)
class Phase:
    seconds: float
    rate: float                    # requests per second for this user; 0 = idle
    pattern: str = "mixed"
    jitter: str = "poisson"        # poisson | constant | uniform


@dataclass(frozen=True)
class Role:
    count: int
    phases: Callable[[random.Random], list[Phase]]
    description: str
    expect: tuple = ()             # ((check, value, quantifier), ...)  see observe.py


@dataclass(frozen=True)
class Scenario:
    title: str
    description: str
    roles: dict[str, Role]
    duration_s: int                # recommended --run-time (before SCN_TIME_SCALE)
    notes: str = ""

    @property
    def users(self) -> int:
        return sum(r.count for r in self.roles.values())


def _fixed(*phases: Phase):
    return lambda rng: list(phases)


def _crowd(rng):
    join = rng.uniform(60, 75)
    return [Phase(join, 0), Phase(60, 2.0, "mixed", "poisson")]


def _random_bot(rng):
    phases = [Phase(30, 2.0, "mixed", "poisson")]                  # starts quietly, then varies
    for _ in range(14):
        phases.append(Phase(rng.uniform(8, 20),
                            rng.choice([0.5, 3.0, 8.0, 15.0, 30.0]),
                            rng.choice(["mixed", "single:/api/search", "scan404", "fail401"]),
                            rng.choice(["poisson", "uniform"])))
    return phases


SCENARIOS: dict[str, Scenario] = {
    "normal": Scenario(
        "Normal client behaviour",
        "Five well-behaved clients, ~2 requests/s each, spread over several endpoints, no errors.",
        {"client": Role(5, _fixed(Phase(150, 2.0)), "ordinary users",
                        (("max_state_at_most", "NORMAL", "all"), ("max_429", 0, "all"),
                         ("scored", True, "all")))},
        duration_s=150),
    "flash_crowd": Scenario(
        "Legitimate flash crowd",
        "5 steady users plus 30 extra users who all arrive within ~15 s, behave normally for 60 s and "
        "leave. Aggregate load jumps ~8x; every individual client stays ordinary.",
        {"steady": Role(5, _fixed(Phase(180, 2.0)), "background users",
                        (("max_state_at_most", "NORMAL", "all"),)),
         "crowd": Role(30, _crowd, "legitimate arrivals (new identities, no history)",
                       (("max_state_at_most", "MONITORED", "all"), ("max_429", 0, "all")))},
        duration_s=180,
        notes="Fairness question: a legitimate crowd must not be throttled or blocked."),
    "sustained_high": Scenario(
        "Sustained high traffic",
        "`ramper`: 70 s of normal use (builds a baseline) then 100 s at 25 req/s (above the 10 req/s "
        "token-bucket refill rate). `cold`: 25 req/s from its first request, no history.",
        {"ramper": Role(2, _fixed(Phase(70, 2.0), Phase(100, 25.0)), "heavy user with history",
                        (("max_state_at_least", "THROTTLED", "all"), ("max_state_at_most", "THROTTLED", "all"),
                         ("min_429", 1, "all"))),
         "cold": Role(1, _fixed(Phase(170, 25.0)), "heavy user without history",
                      (("max_state_at_least", "MONITORED", "all"), ("max_state_at_most", "MONITORED", "all"),
                       ("min_429", 1, "all")))},
        duration_s=170,
        notes="`cold` has no baseline so D is unavailable: B alone gives 25 points (design limitation)."),
    "low_and_slow": Scenario(
        "Low-and-slow automation",
        "`reader` polls ONE endpoint at 0.5 req/s (200 OK). `stuffer` sends 1.5 req/s of failing "
        "logins to one endpoint (401).",
        {"reader": Role(1, _fixed(Phase(170, 0.5, "single:/api/orders")), "slow repetitive poller",
                        # First prediction was "stays NORMAL" (too few requests per span for C). The first
                        # run (s1) REFUTED it: Poisson timing occasionally puts >=10 requests in a 10 s span,
                        # so C=1 and the client flickers into MONITORED. Revised: never worse than MONITORED.
                        (("max_state_at_most", "MONITORED", "all"), ("scored", True, "all"))),
         "stuffer": Role(1, _fixed(Phase(170, 1.5, "fail401")), "slow credential stuffer",
                         (("max_state_at_least", "MONITORED", "all"),))},
        duration_s=170,
        notes="`reader` mostly evades detection (usually <10 requests per 10 s span, so C is "
              "'unavailable') but is flagged MONITORED intermittently when random timing bunches requests. "
              "It is never restricted. `stuffer` is detected."),
    "randomized": Scenario(
        "Randomised automation",
        "Two bots whose rate (0.5-30 req/s), endpoint pattern (mixed/single/404 scan/failing login) and "
        "timing change every 8-20 s, seeded for reproducibility.",
        {"bot": Role(2, _random_bot, "unpredictable automation",
                     (("max_state_at_least", "MONITORED", "any"), ("max_transitions", 14, "all"),
                      ("scored", True, "all")))},
        duration_s=240,
        notes="Checks that scoring reacts and that hysteresis/dwell stop rapid state flapping."),
    "multi_identity": Scenario(
        "Multiple client identities",
        "10 normal users, 3 distributed scrapers (4 req/s of 404s each, aggregate 12 req/s) and 1 "
        "flooder (60 s normal, then 60 req/s at one endpoint for 80 s).",
        {"normal": Role(10, _fixed(Phase(200, 1.5)), "ordinary users",
                        (("max_state_at_most", "MONITORED", "all"),)),
         "scraper": Role(3, _fixed(Phase(200, 4.0, "scan404")), "distributed scrapers",
                         (("max_state_at_least", "MONITORED", "all"), ("max_state_at_most", "MONITORED", "all"))),
         "flooder": Role(1, _fixed(Phase(60, 2.0), Phase(80, 60.0, "single:/api/search"), Phase(60, 0)),
                         "abusive client", (("max_state_at_least", "BLOCKED", "all"), ("min_429", 1, "all")))},
        duration_s=200,
        notes="Each identity is judged on its own; the three scrapers stay under the BLOCKED/THROTTLED "
              "thresholds individually even though together they are noisy."),
}


def run_id() -> str:
    return os.environ.get("SCN_RUN_ID", "r1")


def seed() -> int:
    return int(os.environ.get("SCN_SEED", "1"))


def time_scale() -> float:
    return float(os.environ.get("SCN_TIME_SCALE", "1.0"))


def rate_scale() -> float:
    return float(os.environ.get("SCN_RATE_SCALE", "1.0"))


def role_count(role_name: str, role: Role) -> int:
    return int(os.environ.get(f"SCN_COUNT_{role_name.upper()}", role.count))


SHORT = {"flash_crowd": "flash", "sustained_high": "high", "low_and_slow": "slow",
         "randomized": "rand", "multi_identity": "multi", "normal": "normal"}


def identity(scenario: str, role: str, index: int, rid: str | None = None) -> str:
    return f"lt-{SHORT[scenario]}-{rid or run_id()}-{role}-{index:02d}"


def identities(scenario: str, role_name: str, rid: str | None = None) -> list[str]:
    role = SCENARIOS[scenario].roles[role_name]
    return [identity(scenario, role_name, i, rid) for i in range(role_count(role_name, role))]
