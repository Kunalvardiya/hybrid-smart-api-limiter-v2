"""Wire format of the enforcement table shared by the worker (writer) and gateway (reader).

Redis hash `policy:enforced`: field = client id, value = "STATE:multiplier:updated_ms".
Only clients that are not NORMAL appear. `updated_ms` is refreshed by the worker on every
evaluation, so the gateway can ignore entries when the worker has stopped.
"""
from app.policy.params import State

ENFORCED_HASH = "policy:enforced"
POLICY_EVENTS_STREAM = "policy:events"       # capped log of every state change


def format_enforced(state: State, multiplier: float, now_ms: int) -> str:
    return f"{state.name}:{multiplier:g}:{now_ms}"


def parse_enforced(value: str) -> tuple[State, float, int] | None:
    """Returns (state, multiplier, updated_ms) or None if the value is malformed."""
    try:
        name, mult, updated = value.split(":")
        state, m = State[name], float(mult)
        if not 0.0 <= m <= 1.0:
            return None
        return state, m, int(updated)
    except (ValueError, KeyError):
        return None
