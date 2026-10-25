"""Behaviour engine: telemetry events in, explainable per-client scores out.

No Redis and no wall-clock in here; time is the `win` field (one-second window id)
carried by the events, so the engine is deterministic and easy to test.

How it works
  1. ingest(): events are bucketed per client per one-second window. The newest
     window seen minus `grace_s` is the "closed" boundary: windows up to it are final.
  2. advance(): for every active client, each closed second w is evaluated over
     the span of the last `span_s` windows ending at w:
        rate = requests / span          peak = busiest single second
        D, B, F, C -> risk score -> (maybe) EWMA baseline update
  3. A score is only produced from COMPLETE observation. If the span starts before
     this engine began observing, or overlaps a telemetry "gap", the record has
     status "incomplete" and no score. Silence caused by lost telemetry is never
     scored as a quiet client.
"""
from collections import Counter
from dataclasses import dataclass, field

from app.worker.features import ewma, feature_b, feature_c, feature_d, feature_f
from app.worker.params import ScoringParams
from app.worker.scoring import contributions, risk_score

SCORED, WARMING_UP, INCOMPLETE = "scored", "warming_up", "incomplete"


@dataclass
class WindowStats:
    total: int = 0
    served: int = 0          # reached the protected API (up_status > 0)
    f4xx: int = 0            # protected API answered 400-499
    endpoints: Counter = field(default_factory=Counter)


@dataclass
class ClientState:
    baseline: float | None = None
    obs_count: int = 0                 # evaluations the baseline has learned from
    next_eval: int | None = None
    first_win: int | None = None       # first window in which this client was seen (ramp-in handling)
    counts: dict[int, WindowStats] = field(default_factory=dict)


@dataclass(frozen=True)
class ScoreRecord:
    client: str
    win: int                           # last second of the span this record describes
    status: str                        # scored | warming_up | incomplete
    score: float | None
    D: float | None = None
    B: float | None = None
    F: float | None = None
    C: float | None = None
    rate: float = 0.0                  # requests / second over the span
    peak: int = 0                      # busiest single second in the span
    baseline: float | None = None      # EWMA baseline AFTER this evaluation
    obs_count: int = 0
    n_requests: int = 0
    n_served: int = 0
    n_4xx: int = 0
    top_endpoint: str = ""
    top_share: float = 0.0
    unavailable: tuple[str, ...] = ()  # features that had too little data (counted as 0, not hidden)
    notes: tuple[str, ...] = ()

    @property
    def contributions(self) -> dict[str, float]:
        return contributions(self.D or 0.0, self.B or 0.0, self.F or 0.0, self.C or 0.0)

    def to_fields(self) -> dict[str, str]:
        """Flat string mapping for the Redis hash `policy:{client}`."""
        f = {"schema": "1", "status": self.status, "win": str(self.win),
             "notes": "; ".join(self.notes)}
        if self.status == INCOMPLETE:
            return f            # keep the previous score fields; consumers must check `status`/`scored_win`
        f.update({
            "score": f"{self.score:.2f}", "scored_win": str(self.win),
            "D": f"{self.D:.4f}", "B": f"{self.B:.4f}", "F": f"{self.F:.4f}", "C": f"{self.C:.4f}",
            "rate": f"{self.rate:.3f}", "peak": str(self.peak),
            "baseline": "" if self.baseline is None else f"{self.baseline:.4f}",
            "obs_count": str(self.obs_count),
            "n_requests": str(self.n_requests), "n_served": str(self.n_served), "n_4xx": str(self.n_4xx),
            "top_endpoint": self.top_endpoint, "top_share": f"{self.top_share:.4f}",
            "unavailable": ",".join(self.unavailable),
        })
        return f


class BehaviourEngine:
    def __init__(self, params: ScoringParams | None = None):
        self.p = params or ScoringParams()
        self.clients: dict[str, ClientState] = {}
        self.watermark: int | None = None       # newest request window seen
        self.closed: int | None = None          # windows <= this are final
        self.complete_from: int | None = None   # first window we have observed from the start
        self.gaps: list[tuple[int, int]] = []   # (from_win, to_win) of lost telemetry
        self.late_events = 0
        self.bad_events = 0
        # Returns clients that must stay alive while silent (e.g. blocked clients that
        # still need to be evaluated so they can recover). Set by the worker.
        self.keep_alive = lambda: frozenset()

    # ---- input -------------------------------------------------------------
    def begin_observation(self, first_win: int) -> None:
        """Declare that everything from `first_win` on is observed. Earlier windows are unknown."""
        if self.complete_from is None:
            self.complete_from = first_win

    def seed_client(self, client: str, baseline: float | None, obs_count: int) -> None:
        """Restore a persisted baseline (e.g. after a worker restart)."""
        st = self.clients.setdefault(client, ClientState())
        if st.baseline is None:
            st.baseline, st.obs_count = baseline, obs_count

    def ingest(self, event: dict) -> None:
        try:
            kind = event.get("type")
            if kind == "gap":
                self.gaps.append((int(event["from_ts"]) // 1000, int(event["to_ts"]) // 1000))
            elif kind == "tick":                # gateway heartbeat: moves time forward, carries no data
                self.watermark = max(self.watermark or 0, int(event["win"]))
            elif kind == "req":
                self._ingest_request(event["client"], int(event["win"]), event["endpoint"],
                                     int(event["up_status"]))
            else:
                self.bad_events += 1
        except (KeyError, ValueError, TypeError):
            self.bad_events += 1

    def _ingest_request(self, client: str, win: int, endpoint: str, up_status: int) -> None:
        if self.closed is not None and win <= self.closed:
            self.late_events += 1               # window already final; cannot be applied
            return
        st = self.clients.setdefault(client, ClientState())
        st.next_eval = win if st.next_eval is None else min(st.next_eval, win)
        st.first_win = win if st.first_win is None else min(st.first_win, win)
        w = st.counts.setdefault(win, WindowStats())
        w.total += 1
        w.endpoints[endpoint] += 1
        if up_status > 0:
            w.served += 1
            if 400 <= up_status < 500:
                w.f4xx += 1
        self.watermark = win if self.watermark is None else max(self.watermark, win)

    # ---- evaluation -----------------------------------------------------------
    def advance(self) -> list[ScoreRecord]:
        """Evaluate all newly closed windows; returns the latest record per client."""
        if self.watermark is None:
            return []
        closed = self.watermark - self.p.grace_s
        if self.closed is not None and closed <= self.closed:
            return []
        out: list[ScoreRecord] = []
        pinned = self.keep_alive()
        for client, st in list(self.clients.items()):
            if st.next_eval is None and client in pinned:
                st.next_eval = closed
            if st.next_eval is None or st.next_eval > closed:
                continue
            last, w = None, st.next_eval
            while w <= closed:
                last = self._evaluate(client, st, w)
                quiet_from = (max(st.counts) + self.p.span_s) if st.counts else w
                # Nothing can change until `closed`: skip the empty seconds in between.
                w = closed if (w >= quiet_from and w < closed) else w + 1
            st.next_eval = closed + 1
            for old in [k for k in st.counts if k <= closed - self.p.span_s]:
                del st.counts[old]
            out.append(last)
            if last.n_requests == 0 and not st.counts and client not in pinned:
                del self.clients[client]        # fully idle; baseline is persisted by the caller
        self.closed = closed
        self.gaps = [g for g in self.gaps if g[1] >= closed - self.p.span_s]
        return out

    def _evaluate(self, client: str, st: ClientState, w: int) -> ScoreRecord:
        p = self.p
        start = w - p.span_s + 1

        if self.complete_from is not None and start < self.complete_from:
            return ScoreRecord(client, w, INCOMPLETE, None, baseline=st.baseline, obs_count=st.obs_count,
                               notes=(f"observation began at window {self.complete_from}; "
                                      f"span {start}..{w} not fully observed",))
        for g_from, g_to in self.gaps:
            if g_from <= w and g_to >= start:
                return ScoreRecord(client, w, INCOMPLETE, None, baseline=st.baseline, obs_count=st.obs_count,
                                   notes=(f"telemetry gap {g_from}..{g_to} overlaps span {start}..{w}",))

        per_sec = [st.counts.get(x) for x in range(start, w + 1)]
        totals = [s.total if s else 0 for s in per_sec]
        n = sum(totals)
        served = sum(s.served for s in per_sec if s)
        n4xx = sum(s.f4xx for s in per_sec if s)
        endpoints: Counter = Counter()
        for s in per_sec:
            if s:
                endpoints.update(s.endpoints)
        # Ramp-in: seconds before the client first appeared are not "silence", they are unknown
        # history. Until the client has been seen for a full span, average over the seconds it has
        # actually existed, and do not let that short, noisy estimate teach the baseline.
        age = (w - st.first_win + 1) if st.first_win is not None else p.span_s
        observed_s = max(1, min(p.span_s, age))
        full_span = observed_s >= p.span_s
        rate = n / observed_s
        peak = max(totals)
        top_endpoint, top_count = endpoints.most_common(1)[0] if endpoints else ("", 0)
        top_share = top_count / n if n else 0.0

        notes: list[str] = []
        unavailable: list[str] = []

        # D needs a trusted baseline, but only when there IS traffic: with zero requests in the span the
        # deviation is 0 for any baseline, so the score is complete and a silent client can be released
        # (before this was handled, a restricted client that never built a baseline stayed stuck forever).
        warming = (st.baseline is None or st.obs_count < p.warmup_evals) and n > 0
        if warming:
            d = None
            unavailable.append("D")
            notes.append(f"D unavailable: baseline warming up ({st.obs_count}/{p.warmup_evals})")
        else:
            d = feature_d(rate, st.baseline or 0.0, p)
        b = feature_b(peak, p)
        f = feature_f(n4xx, served, p)
        if f is None:
            unavailable.append("F")
            notes.append(f"F unavailable: {served} upstream responses < {p.min_requests_f}")
        c = feature_c(top_share, n, p)
        if c is None:
            unavailable.append("C")
            notes.append(f"C unavailable: {n} requests < {p.min_requests_c}")

        score = risk_score(d or 0.0, b, f or 0.0, c or 0.0)

        # Learn only from complete, active, low-risk evaluations (prevents baseline poisoning).
        if n > 0 and not full_span:
            notes.append(f"baseline not updated: client seen for {observed_s}s < span {p.span_s}s")
        elif n > 0 and score < p.baseline_update_max_risk:
            st.baseline = rate if st.baseline is None else ewma(st.baseline, rate, p.alpha)
            st.obs_count += 1
        elif n > 0:
            notes.append(f"baseline not updated: score {score:.1f} >= {p.baseline_update_max_risk}")

        return ScoreRecord(
            client, w, WARMING_UP if warming else SCORED, score,
            D=d or 0.0, B=b, F=f or 0.0, C=c or 0.0, rate=rate, peak=peak,
            baseline=st.baseline, obs_count=st.obs_count, n_requests=n, n_served=served, n_4xx=n4xx,
            top_endpoint=top_endpoint, top_share=top_share,
            unavailable=tuple(unavailable), notes=tuple(notes))

