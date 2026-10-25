"""State machine tests: mapping, upward/downward transitions, hysteresis, dwell, recovery, boundaries.

Defaults: enter 25 / 50 / 75, hysteresis 5 (exit 20 / 45 / 70), dwell 10 / 30 / 60 s,
throttle multiplier 0.25. Time is the window id (seconds).
"""
import pytest

from app.policy.engine import PolicyEngine
from app.policy.machine import decide, target_state
from app.policy.params import PolicyParams, State, load_policy_params
from app.policy.store import format_enforced, parse_enforced
from app.worker.engine import ScoreRecord

P = PolicyParams()
N, M, T, B = State.NORMAL, State.MONITORED, State.THROTTLED, State.BLOCKED


# ---- score -> state mapping, boundaries ------------------------------------
@pytest.mark.parametrize("score,expected", [
    (0, N), (24.99, N), (25.0, M), (25.01, M), (49.99, M),
    (50.0, T), (74.99, T), (75.0, B), (100.0, B),
])
def test_target_state_boundaries_are_inclusive_at_the_entry_threshold(score, expected):
    assert target_state(score, P) == expected


def test_multipliers_and_dwell_by_state():
    assert [P.multiplier(s) for s in State] == [1.0, 1.0, 0.25, 0.0]
    assert [P.dwell(s) for s in (M, T, B)] == [10, 30, 60]
    assert [P.exit(s) for s in (M, T, B)] == [20.0, 45.0, 70.0]


# ---- upward transitions --------------------------------------------------------
def test_escalation_is_immediate_and_ignores_dwell():
    d = decide(N, 0, 30.0, "scored", 1, P)
    assert (d.state, d.changed, d.since_win) == (M, True, 1)
    d = decide(M, 1, 55.0, "scored", 2, P)            # only 1 s in MONITORED
    assert d.state == T and d.since_win == 2


def test_escalation_can_skip_levels():
    assert decide(N, 0, 90.0, "scored", 5, P).state == B


def test_warming_up_scores_may_escalate():
    assert decide(N, 0, 60.0, "warming_up", 5, P).state == T


# ---- hysteresis -----------------------------------------------------------------
def test_score_inside_the_hysteresis_band_holds_the_state():
    long_ago = -1000
    for state, score in ((M, 22.0), (T, 47.0), (B, 72.0)):
        d = decide(state, long_ago, score, "scored", 0, P)
        assert d.state == state and not d.changed and "hysteresis" in d.reason


def test_exit_requires_strictly_below_the_exit_threshold():
    assert decide(M, -1000, 20.0, "scored", 0, P).state == M      # exactly 20: hold
    assert decide(M, -1000, 19.99, "scored", 0, P).state == N
    assert decide(T, -1000, 45.0, "scored", 0, P).state == T
    assert decide(T, -1000, 44.99, "scored", 0, P).state == M


def test_scores_flickering_around_a_threshold_do_not_flap():
    state, since, changes = N, 0, 0
    for t, score in enumerate([24, 26, 24, 26, 24, 23, 26, 21, 26, 22, 24] * 3):
        d = decide(state, since, float(score), "scored", t, P)
        changes += d.changed
        state, since = d.state, d.since_win
    assert state == M and changes == 1                 # entered once, never left


# ---- dwell time -------------------------------------------------------------------
def test_dwell_blocks_early_exit_then_allows_it():
    d = decide(T, 100, 0.0, "scored", 110, P)         # 10 s < 30 s
    assert d.state == T and "dwell" in d.reason
    assert decide(T, 100, 0.0, "scored", 129, P).state == T
    d = decide(T, 100, 0.0, "scored", 130, P)         # exactly 30 s: allowed
    assert d.state == M and d.since_win == 130


def test_escalating_again_restarts_dwell():
    d = decide(M, 0, 55.0, "scored", 100, P)
    assert d.state == T and d.since_win == 100


# ---- recovery --------------------------------------------------------------------
def test_recovery_is_gradual_one_level_at_a_time():
    state, since, timeline = B, 0, []
    for t in range(1, 200):                            # abnormal behaviour stopped: score 0
        d = decide(state, since, 0.0, "scored", t, P)
        if d.changed:
            timeline.append((t, d.state))
        state, since = d.state, d.since_win
    assert timeline == [(60, T), (90, M), (100, N)]    # 60 s + 30 s + 10 s


def test_recovery_is_interrupted_by_renewed_abuse():
    state, since = B, 0
    for t in range(1, 61):
        d = decide(state, since, 0.0, "scored", t, P)
        state, since = d.state, d.since_win
    assert state == T
    d = decide(state, since, 80.0, "scored", 70, P)    # abuse resumes
    assert d.state == B


# ---- missing / partial information ------------------------------------------------
def test_incomplete_or_missing_scores_hold_the_state():
    for state in (N, M, T, B):
        d = decide(state, 0, None, "incomplete", 10_000, P)
        assert d.state == state and not d.changed
    assert decide(B, 0, 0.0, "incomplete", 10_000, P).state == B   # a score is ignored if incomplete


def test_warming_up_score_cannot_release_a_client():
    d = decide(B, 0, 0.0, "warming_up", 10_000, P)
    assert d.state == B and "lower bound" in d.reason


# ---- parameters --------------------------------------------------------------------
@pytest.mark.parametrize("bad", [
    dict(enter_monitored=60), dict(enter_blocked=101), dict(hysteresis=-1), dict(hysteresis=25),
    dict(dwell_blocked_s=-1), dict(throttle_multiplier=0), dict(throttle_multiplier=1.5),
])
def test_invalid_parameters_are_rejected(bad):
    with pytest.raises(ValueError):
        PolicyParams(**bad)


def test_params_from_environment():
    p = load_policy_params({"POLICY_ENTER_BLOCKED": "90", "POLICY_THROTTLE_MULTIPLIER": "0.5"})
    assert p.enter_blocked == 90 and p.throttle_multiplier == 0.5 and p.enter_monitored == 25


# ---- PolicyEngine -------------------------------------------------------------------
def rec(client, win, score, status="scored"):
    return ScoreRecord(client, win, status, score)


def test_policy_engine_tracks_clients_and_pins_non_normal_ones():
    pe = PolicyEngine(P)
    assert pe.apply(rec("a", 1, 10.0)).state == N and pe.pinned() == frozenset()
    assert pe.apply(rec("a", 2, 80.0)).state == B
    assert pe.pinned() == {"a"} and pe.state_of("a") == B and pe.state_of("zzz") == N
    pe.apply(rec("a", 70, 0.0))                        # 68 s later: B -> T
    assert pe.state_of("a") == T
    pe.apply(rec("a", 101, 0.0)); pe.apply(rec("a", 112, 0.0))
    assert pe.state_of("a") == N and pe.pinned() == frozenset()


def test_policy_engine_restore():
    pe = PolicyEngine(P)
    pe.restore("a", B, 500)
    assert pe.apply(rec("a", 510, 0.0)).state == B               # dwell not met after restart
    assert pe.apply(rec("a", 560, 0.0)).state == T


# ---- Redis wire format --------------------------------------------------------------
def test_enforced_roundtrip_and_malformed_values():
    assert parse_enforced(format_enforced(T, 0.25, 1234)) == (T, 0.25, 1234)
    assert parse_enforced(format_enforced(B, 0.0, 9)) == (B, 0.0, 9)
    for bad in ("", "BLOCKED", "NOPE:1:2", "BLOCKED:x:2", "BLOCKED:2:5", "BLOCKED:0:y", "A:B:C:D"):
        assert parse_enforced(bad) is None
