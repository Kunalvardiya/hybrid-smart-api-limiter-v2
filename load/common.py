"""Locust plumbing shared by every scenario file: safety guards, the phased user, result files."""
import json
import os
import random
import time
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlparse

from locust import HttpUser, events
from locust.exception import StopUser

from load.definitions import (MIXED, SCENARIOS, Phase, identity, rate_scale, role_count, run_id, seed,
                            time_scale)

RESULTS_DIR = Path(__file__).parent / "results"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
MAX_USERS = 200
MAX_USER_RATE = float(os.environ.get("MAX_USER_RATE", "100"))

# identity -> counters, written to load/results/<scenario>-<run_id>-client.json at the end
COUNTS: dict[str, dict] = defaultdict(lambda: {"role": "", "sent": 0, "status": Counter(),
                                              "policy": Counter(), "errors": 0, "bypassed": 0})
SCENARIO_NAME = {"name": None}


# ---- safety ------------------------------------------------------------------------------
def check_safe(environment) -> None:
    """Refuse to run against anything but this machine, and cap the number of users."""
    host = (environment.host or "")
    if host:
        name = urlparse(host).hostname
        if name not in LOCAL_HOSTS:
            raise SystemExit(f"REFUSING to generate load against '{host}': these scenarios only run "
                             f"against localhost / 127.0.0.1 (they are abuse simulations).")
    users = getattr(getattr(environment, "parsed_options", None), "num_users", None)
    if users and users > MAX_USERS:
        raise SystemExit(f"REFUSING: {users} users requested; the limit for these scenarios is {MAX_USERS}.")


@events.init.add_listener
def _on_init(environment, **_kw):
    check_safe(environment)


@events.test_start.add_listener
def _on_start(environment, **_kw):
    check_safe(environment)
    print(f"[load] scenario={SCENARIO_NAME['name']} run_id={run_id()} seed={seed()} "
          f"time_scale={time_scale()} rate_scale={rate_scale()}")


@events.test_stop.add_listener
def _on_stop(environment, **_kw):
    if not SCENARIO_NAME["name"]:
        return
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"{SCENARIO_NAME['name']}-{run_id()}-client.json"
    payload = {ident: {**c, "status": dict(c["status"]), "policy": dict(c["policy"])}
               for ident, c in COUNTS.items()}
    out.write_text(json.dumps({"scenario": SCENARIO_NAME["name"], "run_id": run_id(), "seed": seed(),
                               "time_scale": time_scale(), "rate_scale": rate_scale(),
                               "clients": payload}, indent=1))
    print(f"[load] client-side counters written to {out}")


# ---- the user --------------------------------------------------------------------------------
class ScenarioUser(HttpUser):
    """One Locust user = one client identity following its role's phases."""
    abstract = True
    scenario = ""
    role = ""
    _next_index = 0

    def wait_time(self):
        phase = self._phase()
        if phase is None or phase.rate <= 0:
            return 0.5
        rate = min(phase.rate * rate_scale(), MAX_USER_RATE)
        if phase.jitter == "constant":
            return 1.0 / rate
        if phase.jitter == "uniform":
            return self.rng.uniform(0.5, 1.5) / rate
        return self.rng.expovariate(rate)

    def on_start(self):
        cls = type(self)
        count = max(1, role_count(self.role, SCENARIOS[self.scenario].roles[self.role]))
        self.index = cls._next_index % count
        cls._next_index += 1
        self.identity = identity(self.scenario, self.role, self.index)
        self.rng = random.Random(f"{seed()}:{self.identity}")
        self.phases = SCENARIOS[self.scenario].roles[self.role].phases(self.rng)
        self.t0 = time.monotonic()
        self.total = sum(p.seconds for p in self.phases) * time_scale()
        COUNTS[self.identity]["role"] = self.role

    def _phase(self) -> Phase | None:
        elapsed, scale = time.monotonic() - self.t0, time_scale()
        for p in self.phases:
            if elapsed < p.seconds * scale:
                return p
            elapsed -= p.seconds * scale
        return None

    def _pick(self, pattern: str) -> tuple[str, str]:
        """returns (path to request, name to group it under in Locust statistics)"""
        if pattern.startswith("single:"):
            path = pattern.split(":", 1)[1]
            return path, path.split("?")[0]
        if pattern == "scan404":
            return f"/api/probe/p{self.rng.randrange(10)}", "/api/probe/[x]"
        if pattern == "fail401":
            return "/api/fail/401", "/api/fail/401"
        paths, weights = zip(*MIXED)
        path = self.rng.choices(paths, weights)[0]
        return path.replace("{n}", str(self.rng.randrange(1, 500))), path.replace("{n}", "[n]").split("?")[0]

    @staticmethod
    def _expected(status: int, pattern: str) -> bool:
        if status in (200, 429):                                  # 429 = the limiter doing its job
            return True
        return status in (401, 404) and pattern in ("scan404", "fail401")

    def hit(self):
        phase = self._phase()
        if phase is None:
            raise StopUser()                                      # scenario over for this identity
        if phase.rate <= 0:
            return                                                # idle phase
        path, group = self._pick(phase.pattern)
        c = COUNTS[self.identity]
        with self.client.get(path, headers={"X-API-Key": self.identity}, name=group,
                             catch_response=True) as resp:
            c["sent"] += 1
            c["status"][str(resp.status_code)] += 1
            state = resp.headers.get("X-Policy-State", "NORMAL") if resp.status_code else "ERROR"
            c["policy"][state] += 1
            if resp.headers.get("X-RateLimit-Status") == "bypassed":
                c["bypassed"] += 1                                # limiter failed open (Redis error)
            if self._expected(resp.status_code, phase.pattern):
                resp.success()
            else:
                c["errors"] += 1
                resp.failure(f"unexpected status {resp.status_code}")


def build_user_classes(scenario: str) -> dict:
    """Create one Locust user class per role; the scenario files export them into their namespace."""
    from locust import task
    SCENARIO_NAME["name"] = scenario
    classes = {}
    for role_name, role in SCENARIOS[scenario].roles.items():
        name = f"{role_name.title().replace('_', '')}User"
        classes[name] = type(name, (ScenarioUser,), {
            "__doc__": role.description, "scenario": scenario, "role": role_name,
            "abstract": False, "fixed_count": role_count(role_name, role),
            "hit": ScenarioUser.hit, "tasks": [ScenarioUser.hit],
        })
    return classes

