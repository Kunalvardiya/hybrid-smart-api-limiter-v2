"""Per-client policy state, driven by score records from the behaviour engine."""
from dataclasses import dataclass

from app.policy.machine import Decision, decide
from app.policy.params import PolicyParams, State


@dataclass
class ClientPolicy:
    state: State
    since_win: int


class PolicyEngine:
    def __init__(self, params: PolicyParams | None = None):
        self.p = params or PolicyParams()
        self._clients: dict[str, ClientPolicy] = {}     # only clients that are NOT NORMAL

    def restore(self, client: str, state: State, since_win: int) -> None:
        if state != State.NORMAL:
            self._clients[client] = ClientPolicy(state, since_win)

    def pinned(self) -> frozenset[str]:
        """Clients that must keep being evaluated even if they go silent (so they can recover)."""
        return frozenset(self._clients)

    def state_of(self, client: str) -> State:
        cp = self._clients.get(client)
        return cp.state if cp else State.NORMAL

    def apply(self, record) -> Decision:
        """`record` is an app.worker.engine.ScoreRecord."""
        cp = self._clients.get(record.client)
        current = cp.state if cp else State.NORMAL
        since = cp.since_win if cp else record.win
        d = decide(current, since, record.score, record.status, record.win, self.p)
        if d.state == State.NORMAL:
            self._clients.pop(record.client, None)
        else:
            self._clients[record.client] = ClientPolicy(d.state, d.since_win)
        return d
