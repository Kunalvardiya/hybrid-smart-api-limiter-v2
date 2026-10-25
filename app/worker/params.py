"""Scoring parameters.

The source documents fix the score formula, the 0.25 weights, the 1-second
window, the use of an EWMA baseline and the [0,1] range of D, B, F, C. They do
NOT give numeric values for anything below. Every number here is therefore an
IMPLEMENTATION CHOICE: it is configurable via environment variables and
documented in docs/SCORING.md.
"""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ScoringParams:
    span_s: int = 10                 # features use the last N closed 1-second windows
    alpha: float = 0.02              # EWMA smoothing; one update per second => ~50 s memory
    d_saturation: float = 2.0        # D reaches 1 when rate = (1 + this) x baseline
    rate_floor: float = 1.0          # req/s; minimum baseline used as the divisor in D
    burst_ref: float = 10.0          # req/s; B is 0 at/below this peak, 1 at/above 2x this
    c_floor: float = 0.5             # top-endpoint share at/below this gives C = 0
    min_requests_c: int = 10         # fewer requests in the span => C unavailable
    min_requests_f: int = 5          # fewer upstream responses in the span => F unavailable
    warmup_evals: int = 30           # "normal" evaluations before the baseline is trusted
    baseline_update_max_risk: float = 20.0   # baseline only learns below this score (one saturated feature = 25)
    grace_s: int = 2                 # a window closes when the stream has moved this far past it

    def __post_init__(self):
        if self.span_s < 1 or self.grace_s < 0 or self.warmup_evals < 0:
            raise ValueError("span_s >= 1, grace_s >= 0, warmup_evals >= 0 required")
        if not 0 < self.alpha <= 1:
            raise ValueError("alpha must be in (0, 1]")
        if self.d_saturation <= 0 or self.rate_floor <= 0 or self.burst_ref <= 0:
            raise ValueError("d_saturation, rate_floor, burst_ref must be > 0")
        if not 0 <= self.c_floor < 1:
            raise ValueError("c_floor must be in [0, 1)")


def load_params(env=os.environ) -> ScoringParams:
    d = ScoringParams()
    # burst_ref defaults to the limiter's sustained rate, so "B > 0" means "above what the token bucket allows per second".
    burst_default = float(env.get("BUCKET_REFILL_PER_SEC", d.burst_ref))
    return ScoringParams(
        span_s=int(env.get("SCORING_SPAN_S", d.span_s)),
        alpha=float(env.get("SCORING_ALPHA", d.alpha)),
        d_saturation=float(env.get("SCORING_D_SATURATION", d.d_saturation)),
        rate_floor=float(env.get("SCORING_RATE_FLOOR", d.rate_floor)),
        burst_ref=float(env.get("SCORING_BURST_REF", burst_default)),
        c_floor=float(env.get("SCORING_C_FLOOR", d.c_floor)),
        min_requests_c=int(env.get("SCORING_MIN_REQUESTS_C", d.min_requests_c)),
        min_requests_f=int(env.get("SCORING_MIN_REQUESTS_F", d.min_requests_f)),
        warmup_evals=int(env.get("SCORING_WARMUP_EVALS", d.warmup_evals)),
        baseline_update_max_risk=float(env.get("SCORING_BASELINE_MAX_RISK", d.baseline_update_max_risk)),
        grace_s=int(env.get("SCORING_GRACE_S", d.grace_s)),
    )
