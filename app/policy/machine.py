"""The enforcement state machine. Pure and deterministic: no Redis, no clock.

Time is measured in window ids (event-time seconds), the same clock the scorer uses.

Rules
  UP    If the score reaches the entry threshold of a higher state, move there at once
        (possibly several levels), whatever the dwell time. Reacting quickly to abuse is
        the point of the system.
  DOWN  Move down ONE level only when all hold:
          1. the score is below the state's exit threshold (entry - hysteresis),
          2. the client has been in the state for at least its minimum dwell time,
          3. the score is fully informed (status "scored"; a "warming_up" score is only
             a lower bound and must not release a client).
        Each step down restarts the dwell timer, so recovery is gradual:
        BLOCKED -> THROTTLED -> MONITORED -> NORMAL.
  HOLD  Anything else, including scores in the hysteresis band and records without a
        usable score (status "incomplete"): the state does not change.
"""
from dataclasses import dataclass

from app.policy.params import PolicyParams, State

SCORED, WARMING_UP = "scored", "warming_up"


@dataclass(frozen=True)
class Decision:
    state: State
    previous: State
    since_win: int            # window in which `state` was entered
    reason: str

    @property
    def changed(self) -> bool:
        return self.state != self.previous


def target_state(score: float, p: PolicyParams) -> State:
    """The state a score maps to when escalating (>= is inclusive)."""
    for s in (State.BLOCKED, State.THROTTLED, State.MONITORED):
        if score >= p.enter(s):
            return s
    return State.NORMAL


def decide(current: State, since_win: int, score: float | None, status: str,
           now_win: int, p: PolicyParams) -> Decision:
    def hold(why: str) -> Decision:
        return Decision(current, current, since_win, why)

    if score is None or status not in (SCORED, WARMING_UP):
        return hold(f"no usable score (status={status}); state held")

    target = target_state(score, p)
    if target > current:
        return Decision(target, current, now_win,
                        f"score {score:.1f} >= {p.enter(target):g} (enter {target.name})")

    if current == State.NORMAL:
        return hold("score below every threshold")

    exit_at = p.exit(current)
    if score >= exit_at:
        return hold(f"score {score:.1f} not below exit threshold {exit_at:g} for {current.name} "
                    f"(hysteresis band)")
    if status != SCORED:
        return hold(f"score {score:.1f} is only a lower bound ({status}); not releasing {current.name}")
    waited = now_win - since_win
    if waited < p.dwell(current):
        return hold(f"score {score:.1f} < {exit_at:g} but dwell {waited}s < {p.dwell(current)}s "
                    f"in {current.name}")
    lower = State(current - 1)
    return Decision(lower, current, now_win,
                    f"score {score:.1f} < {exit_at:g} and dwell {waited}s >= {p.dwell(current)}s "
                    f"(leave {current.name})")
