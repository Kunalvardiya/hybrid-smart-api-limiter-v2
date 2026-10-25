"""Unit tests for the pure scoring pieces: EWMA, D/B/F/C normalisation, the risk formula.

Defaults used throughout: span 10 s, d_saturation 2, rate_floor 1, burst_ref 10,
c_floor 0.5, min_requests_c 10, min_requests_f 5 (see docs/SCORING.md).
"""
import pytest

from app.worker.features import clip01, ewma, feature_b, feature_c, feature_d, feature_f
from app.worker.params import ScoringParams, load_params
from app.worker.scoring import WEIGHTS, contributions, risk_score

P = ScoringParams()


def test_clip01():
    assert (clip01(-3), clip01(0.4), clip01(7)) == (0.0, 0.4, 1.0)


def test_ewma_update():
    assert ewma(10.0, 20.0, 0.1) == pytest.approx(11.0)
    assert ewma(10.0, 10.0, 0.5) == 10.0               # steady input leaves the baseline alone
    assert ewma(10.0, 20.0, 1.0) == 20.0               # alpha=1 follows the sample
    x = 0.0
    for _ in range(500):
        x = ewma(x, 8.0, 0.05)
    assert x == pytest.approx(8.0, abs=1e-6)            # converges to a constant input


@pytest.mark.parametrize("rate,baseline,expected", [
    (5, 5, 0.0),          # at baseline
    (2, 5, 0.0),          # below baseline: negative deviation clipped to 0
    (10, 5, 0.5),         # (10-5)/(2*5)
    (15, 5, 1.0),         # 3x baseline saturates
    (100, 5, 1.0),        # far above: still capped at 1
    (3, 0.2, 1.0),        # tiny baseline uses rate_floor=1 as divisor: (3-0.2)/2 -> clipped
    (1.5, 0.2, 0.65),     # (1.5-0.2)/(2*1)
    (0, 0, 0.0),
])
def test_feature_d(rate, baseline, expected):
    assert feature_d(rate, baseline, P) == pytest.approx(expected)


@pytest.mark.parametrize("peak,expected", [(0, 0.0), (10, 0.0), (15, 0.5), (20, 1.0), (500, 1.0)])
def test_feature_b(peak, expected):
    assert feature_b(peak, P) == pytest.approx(expected)


@pytest.mark.parametrize("n4,served,expected", [
    (0, 50, 0.0), (5, 50, 0.1), (50, 50, 1.0), (5, 5, 1.0),
    (4, 4, None),         # fewer than 5 upstream responses: not enough evidence
    (0, 0, None),
])
def test_feature_f(n4, served, expected):
    result = feature_f(n4, served, P)
    assert result == expected if expected is None else result == pytest.approx(expected)


@pytest.mark.parametrize("share,n,expected", [
    (0.25, 40, 0.0),      # spread over 4 endpoints
    (0.5, 40, 0.0),       # at the floor
    (0.75, 40, 0.5),      # (0.75-0.5)/0.5
    (1.0, 40, 1.0),       # everything to one endpoint
    (1.0, 9, None),       # too few requests to call it "concentrated"
    (0.0, 0, None),
])
def test_feature_c(share, n, expected):
    result = feature_c(share, n, P)
    assert result == expected if expected is None else result == pytest.approx(expected)


def test_weights_match_the_specification():
    assert WEIGHTS == {"D": 0.25, "B": 0.25, "F": 0.25, "C": 0.25}
    assert sum(WEIGHTS.values()) == 1.0


@pytest.mark.parametrize("d,b,f,c,expected", [
    (0, 0, 0, 0, 0.0),
    (1, 1, 1, 1, 100.0),
    (1, 0, 0, 0, 25.0),
    (0.5, 0.5, 0.5, 0.5, 50.0),
    (1, 1, 0, 0, 50.0),
    (0.4, 0.8, 0.2, 0.6, 50.0),      # 100 x 0.25 x (0.4+0.8+0.2+0.6) = 50
    (0.2, 0.0, 1.0, 0.4, 40.0),
])
def test_risk_score_formula(d, b, f, c, expected):
    assert risk_score(d, b, f, c) == pytest.approx(expected)


@pytest.mark.parametrize("bad", [-0.01, 1.01])
def test_risk_score_rejects_unnormalised_features(bad):
    with pytest.raises(ValueError):
        risk_score(bad, 0, 0, 0)


def test_contributions_sum_to_score_and_name_the_cause():
    parts = contributions(0.4, 0.8, 0.2, 0.6)
    assert parts == pytest.approx({"D": 10.0, "B": 20.0, "F": 5.0, "C": 15.0})
    assert sum(parts.values()) == pytest.approx(risk_score(0.4, 0.8, 0.2, 0.6))


def test_score_is_deterministic():
    assert risk_score(0.37, 0.91, 0.05, 0.66) == risk_score(0.37, 0.91, 0.05, 0.66)


def test_params_validation():
    for bad in (dict(span_s=0), dict(alpha=0), dict(alpha=1.5), dict(burst_ref=0), dict(c_floor=1.0)):
        with pytest.raises(ValueError):
            ScoringParams(**bad)


def test_params_from_environment_defaults_burst_ref_to_limiter_rate():
    p = load_params({"BUCKET_REFILL_PER_SEC": "25", "SCORING_ALPHA": "0.1"})
    assert p.burst_ref == 25.0 and p.alpha == 0.1
    assert load_params({"BUCKET_REFILL_PER_SEC": "25", "SCORING_BURST_REF": "7"}).burst_ref == 7.0
