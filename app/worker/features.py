"""Pure functions: EWMA and the four normalised features D, B, F, C.

Each feature returns a float in [0, 1], or None when there is not enough data to
compute it honestly. None is never silently turned into a number here; the
engine decides how to report it.
"""
from app.worker.params import ScoringParams


def clip01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x


def ewma(previous: float, sample: float, alpha: float) -> float:
    """new = alpha * sample + (1 - alpha) * previous"""
    return alpha * sample + (1 - alpha) * previous


def feature_d(rate: float, baseline: float, p: ScoringParams) -> float:
    """Rate deviation: how far above its own baseline the client's request rate is.

    D = clip01( (rate - baseline) / (d_saturation * max(baseline, rate_floor)) )
    Rate at or below baseline => 0; rate = (1 + d_saturation) x baseline => 1.
    """
    return clip01((rate - baseline) / (p.d_saturation * max(baseline, p.rate_floor)))


def feature_b(peak_per_s: float, p: ScoringParams) -> float:
    """Burst intensity: busiest single second in the span versus burst_ref.

    B = clip01( (peak - burst_ref) / burst_ref )   -> 0 at <= burst_ref, 1 at >= 2 x burst_ref
    """
    return clip01((peak_per_s - p.burst_ref) / p.burst_ref)


def feature_f(n_4xx: int, n_served: int, p: ScoringParams) -> float | None:
    """Failure rate: upstream 4xx responses / requests that reached the protected API.

    The gateway's own 429s are not in the numerator or the denominator, so being
    throttled can neither raise nor dilute F.
    """
    if n_served < p.min_requests_f:
        return None
    return clip01(n_4xx / n_served)


def feature_c(top_share: float, n_requests: int, p: ScoringParams) -> float | None:
    """Endpoint concentration: share of requests hitting the single most-used endpoint, rescaled.

    C = clip01( (top_share - c_floor) / (1 - c_floor) )
    """
    if n_requests < p.min_requests_c:
        return None
    return clip01((top_share - p.c_floor) / (1 - p.c_floor))
