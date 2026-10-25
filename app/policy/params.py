"""Policy parameters: score thresholds, hysteresis, dwell times, throttle strength.

SOURCE-DEFINED: the four states NORMAL -> MONITORED -> THROTTLED -> BLOCKED, and that
transitions use "risk thresholds, hysteresis, and a minimum dwell time", with access
"progressively restored" on recovery.
IMPLEMENTATION CHOICES (everything numeric below): the documents give no values.
They are configurable via POLICY_* environment variables; see docs/POLICY.md.

The default thresholds follow the structure of the score: each of D, B, F, C adds at
most 25 points, so
    >= 25  one signal fully saturated      -> MONITORED
    >= 50  two signals fully saturated     -> THROTTLED
    >= 75  three signals fully saturated   -> BLOCKED
"""
import os
from dataclasses import dataclass
from enum import IntEnum


class State(IntEnum):
    NORMAL = 0
    MONITORED = 1
    THROTTLED = 2
    BLOCKED = 3


@dataclass(frozen=True)
class PolicyParams:
    enter_monitored: float = 25.0
    enter_throttled: float = 50.0
    enter_blocked: float = 75.0
    hysteresis: float = 5.0            # leave a state only below (its entry threshold - this)
    dwell_monitored_s: int = 10        # minimum time in a state before it may be LEFT downward
    dwell_throttled_s: int = 30
    dwell_blocked_s: int = 60
    throttle_multiplier: float = 0.25  # THROTTLED clients get this share of capacity and refill rate

    def __post_init__(self):
        if not 0 < self.enter_monitored < self.enter_throttled < self.enter_blocked <= 100:
            raise ValueError("need 0 < enter_monitored < enter_throttled < enter_blocked <= 100")
        gaps = (self.enter_monitored, self.enter_throttled - self.enter_monitored,
                self.enter_blocked - self.enter_throttled)
        if not 0 <= self.hysteresis < min(gaps):
            raise ValueError("hysteresis must be >= 0 and smaller than the gap between thresholds")
        if min(self.dwell_monitored_s, self.dwell_throttled_s, self.dwell_blocked_s) < 0:
            raise ValueError("dwell times must be >= 0")
        if not 0 < self.throttle_multiplier <= 1:
            raise ValueError("throttle_multiplier must be in (0, 1]")

    def enter(self, state: State) -> float:
        return {State.MONITORED: self.enter_monitored, State.THROTTLED: self.enter_throttled,
                State.BLOCKED: self.enter_blocked}[state]

    def exit(self, state: State) -> float:
        """Score the client must fall BELOW to leave `state` downward."""
        return self.enter(state) - self.hysteresis

    def dwell(self, state: State) -> int:
        return {State.MONITORED: self.dwell_monitored_s, State.THROTTLED: self.dwell_throttled_s,
                State.BLOCKED: self.dwell_blocked_s}[state]

    def multiplier(self, state: State) -> float:
        """Fraction of the normal token bucket the gateway applies. 0 = no requests pass."""
        return {State.NORMAL: 1.0, State.MONITORED: 1.0, State.THROTTLED: self.throttle_multiplier,
                State.BLOCKED: 0.0}[state]


def load_policy_params(env=os.environ) -> PolicyParams:
    d = PolicyParams()
    return PolicyParams(
        enter_monitored=float(env.get("POLICY_ENTER_MONITORED", d.enter_monitored)),
        enter_throttled=float(env.get("POLICY_ENTER_THROTTLED", d.enter_throttled)),
        enter_blocked=float(env.get("POLICY_ENTER_BLOCKED", d.enter_blocked)),
        hysteresis=float(env.get("POLICY_HYSTERESIS", d.hysteresis)),
        dwell_monitored_s=int(env.get("POLICY_DWELL_MONITORED_S", d.dwell_monitored_s)),
        dwell_throttled_s=int(env.get("POLICY_DWELL_THROTTLED_S", d.dwell_throttled_s)),
        dwell_blocked_s=int(env.get("POLICY_DWELL_BLOCKED_S", d.dwell_blocked_s)),
        throttle_multiplier=float(env.get("POLICY_THROTTLE_MULTIPLIER", d.throttle_multiplier)),
    )
