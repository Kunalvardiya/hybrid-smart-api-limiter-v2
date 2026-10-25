"""Behaviour engine tests with synthetic, deterministic telemetry (no Redis, no clock)."""
import pytest

from app.worker.engine import INCOMPLETE, SCORED, WARMING_UP, BehaviourEngine
from app.worker.params import ScoringParams

T0 = 1000                       # first window id in every scenario
ENDPOINTS4 = ["/api/a", "/api/b", "/api/c", "/api/d"]


def req(client, win, endpoint, up_status=200):
    return {"type": "req", "client": client, "win": str(win), "endpoint": endpoint,
            "up_status": str(up_status)}


def second(client, win, per_second, endpoints=ENDPOINTS4, up_status=200):
    return [req(client, win, endpoints[i % len(endpoints)], up_status) for i in range(per_second)]


def run(engine, schedule, *, extra=None):
    """schedule: list of (win, [events]). Feed one second at a time, advance after each.
    extra: {win: [events]} injected BEFORE that second's traffic (e.g. gap events)."""
    records = []
    for win, events in schedule:
        for e in (extra or {}).get(win, []):
            engine.ingest(e)
        for e in events:
            engine.ingest(e)
        records.extend(engine.advance())
    return records


def steady(client, start, seconds, rate, **kw):
    return [(w, second(client, w, rate, **kw)) for w in range(start, start + seconds)]


def make(**params):
    e = BehaviourEngine(ScoringParams(**params))
    e.begin_observation(T0)
    return e


def last(records, client="c"):
    return [r for r in records if r.client == client][-1]


def test_cold_start_is_incomplete_until_a_full_span_is_observed():
    recs = run(make(), steady("c", T0, 15, 5))
    by_win = {r.win: r for r in recs}
    assert all(by_win[w].status == INCOMPLETE and by_win[w].score is None for w in range(T0, T0 + 9) if w in by_win)
    assert by_win[T0 + 9].status != INCOMPLETE          # span T0..T0+9 is the first complete one


def test_warmup_reports_d_unavailable_and_never_hides_it():
    recs = run(make(), steady("c", T0, 25, 5))
    r = last(recs)
    assert r.status == WARMING_UP
    assert "D" in r.unavailable and any("D unavailable" in n for n in r.notes)


def test_normal_traffic_scores_zero_and_baseline_converges():
    r = last(run(make(), steady("c", T0, 120, 5)))
    assert r.status == SCORED
    assert (r.D, r.B, r.F, r.C) == (0.0, 0.0, 0.0, 0.0)
    assert r.score == 0.0
    assert r.baseline == pytest.approx(5.0)
    assert r.rate == pytest.approx(5.0) and r.peak == 5


def test_new_client_ramp_in_does_not_teach_a_too_low_baseline():
    """Regression (found by the Locust flash-crowd run): the seconds before a client first appeared
    must not count as silence, or its baseline starts near 0 and D reads ~1 for perfectly normal use."""
    sched = [(w, second("other", w, 1)) for w in range(T0, T0 + 20)] + \
            [(w, second("late", w, 5) + second("other", w, 1)) for w in range(T0 + 20, T0 + 120)]
    recs = [r for r in run(make(warmup_evals=10), sched) if r.client == "late"]
    scored = [r for r in recs if r.status == SCORED]
    assert scored and all(r.D < 0.05 for r in scored)
    assert scored[-1].baseline == pytest.approx(5.0, abs=0.1)


def test_irregular_but_ordinary_traffic_does_not_inflate_d():
    pattern = [1, 3, 2, 4, 2, 3, 1, 4, 3, 2]                    # 2.5 req/s on average, jittery
    sched = [(w, second("c", w, pattern[w % 10])) for w in range(T0, T0 + 150)]
    scored = [r for r in run(make(warmup_evals=10), sched) if r.status == SCORED]
    assert scored and max(r.D for r in scored) < 0.2


def test_flood_raises_d_and_b_and_the_baseline_is_not_poisoned():
    schedule = steady("c", T0, 80, 5) + steady("c", T0 + 80, 30, 50)
    recs = run(make(), schedule)
    before = [r for r in recs if r.win == T0 + 79][0]       # last second before the flood
    r = last(recs)
    assert (r.D, r.B) == (1.0, 1.0)
    assert r.F == 0.0 and r.C == 0.0                       # spread over 4 endpoints, no failures
    assert r.score == pytest.approx(50.0)                  # 100 x 0.25 x (1 + 1 + 0 + 0)
    assert r.baseline == pytest.approx(before.baseline)    # unchanged while risk is high
    assert r.obs_count == before.obs_count                 # nothing learned during the flood
    assert r.contributions == pytest.approx({"D": 25.0, "B": 25.0, "F": 0.0, "C": 0.0})


def test_failure_scanning_across_many_endpoints_scores_via_f_only():
    eps = [f"/api/e{i}" for i in range(10)]
    r = last(run(make(), steady("c", T0, 60, 10, endpoints=eps, up_status=404)))
    assert r.F == 1.0 and r.C == 0.0 and r.B == 0.0
    assert r.status == WARMING_UP and r.unavailable == ("D",)
    assert r.score == pytest.approx(25.0)


def test_silent_client_without_a_baseline_gets_a_complete_zero_score():
    """Regression (found on the Docker stack): a scanner that never built a baseline kept a 'warming_up'
    lower-bound score after it went silent, so the policy engine could never release it."""
    eps = [f"/api/e{i}" for i in range(10)]
    sched = [(w, second("scan", w, 10, endpoints=eps, up_status=404) + second("other", w, 1)) for w in range(T0, T0 + 40)] + \
            [(w, second("other", w, 1)) for w in range(T0 + 40, T0 + 70)]
    r = last(run(make(), sched), "scan")
    assert r.n_requests == 0 and r.score == 0.0
    assert r.status == SCORED and "D" not in r.unavailable


def test_single_endpoint_hammering_scores_via_c():
    r = last(run(make(), steady("c", T0, 60, 10, endpoints=["/api/only"])))
    assert r.C == 1.0 and r.top_endpoint == "/api/only" and r.top_share == 1.0
    assert r.score == pytest.approx(25.0)


def test_gateway_429s_count_as_demand_but_not_as_failures():
    sched = [(w, second("c", w, 20, up_status=0)) for w in range(T0, T0 + 30)]   # all limited
    r = last(run(make(), sched))
    assert r.rate == pytest.approx(20.0)
    assert r.n_requests == 200 and r.n_served == 0 and r.n_4xx == 0
    assert r.F == 0.0 and "F" in r.unavailable             # no upstream answers: F honestly unavailable
    assert r.B == 1.0                                       # but the demand shows up as burst


def test_insufficient_telemetry_is_flagged_not_scored_as_certain():
    sched = [(w, second("c", w, 1 if w == T0 + 5 else 0) + second("other", w, 3)) for w in range(T0, T0 + 17)]
    r = last(run(make(), sched))
    assert r.n_requests == 1
    assert set(r.unavailable) >= {"F", "C"}
    assert r.score == 0.0 and any("F unavailable" in n for n in r.notes)


def test_telemetry_gap_makes_overlapping_windows_incomplete_and_freezes_the_baseline():
    gap = {"type": "gap", "from_ts": str(1050 * 1000), "to_ts": str(1052 * 1000 + 999), "lost": "30"}
    sched = steady("c", T0, 50, 5) + steady("c", 1053, 40, 5)      # seconds 1050-1052 were lost
    recs = run(make(), sched, extra={1053: [gap]})
    in_gap = [r for r in recs if 1050 <= r.win <= 1061]
    assert in_gap and all(r.status == INCOMPLETE and r.score is None for r in in_gap)
    assert all("gap" in r.notes[0] for r in in_gap)
    after = [r for r in recs if r.win >= 1062]
    assert after and all(r.status in (SCORED, WARMING_UP) and r.score == 0.0 for r in after)
    pre = [r for r in recs if r.win < 1050][-1]
    assert after[0].obs_count >= pre.obs_count               # nothing learned from the gap


def test_missing_telemetry_without_a_gap_report_is_not_evaluated_as_quiet():
    # No events at all arrive for 3 seconds and no gap event: the watermark does not move,
    # so no window is evaluated and no "quiet" score can be produced from silence.
    eng = make()
    run(eng, steady("c", T0, 30, 5))
    closed_before = eng.closed
    assert eng.advance() == []
    assert eng.closed == closed_before


def test_idle_client_decays_to_zero_and_is_released():
    eng = make()
    sched = steady("c", T0, 30, 20, endpoints=["/api/x"]) + steady("other", T0 + 30, 40, 1)
    recs = run(eng, sched)
    r = last(recs, "c")
    assert r.n_requests == 0 and r.score == 0.0 and r.status != INCOMPLETE
    assert "c" not in eng.clients


def test_tick_events_move_time_forward_so_a_silent_client_is_seen_to_be_quiet():
    eng = make()
    run(eng, steady("c", T0, 30, 20, endpoints=["/api/x"]))
    ticks = [(w, [{"type": "tick", "win": str(w)}]) for w in range(T0 + 30, T0 + 70)]
    r = last(run(eng, ticks), "c")
    assert r.n_requests == 0 and r.score == 0.0 and r.status != INCOMPLETE
    assert eng.bad_events == 0 and eng.closed == T0 + 67


def test_late_events_are_counted_not_applied():
    eng = make()
    run(eng, steady("c", T0, 20, 5))
    n_before = eng.late_events
    eng.ingest(req("c", T0 + 3, "/api/a"))
    assert eng.late_events == n_before + 1


def test_malformed_events_are_ignored_and_counted():
    eng = make()
    for bad in ({"type": "req"}, {"type": "req", "client": "c", "win": "x", "endpoint": "e", "up_status": "1"},
                {"type": "zzz"}, {}):
        eng.ingest(bad)
    assert eng.bad_events == 4 and eng.watermark is None


def test_restart_style_seeding_restores_baseline():
    eng = make(warmup_evals=5)
    eng.seed_client("c", baseline=5.0, obs_count=100)
    r = last(run(eng, steady("c", T0, 40, 20)))
    assert r.status == SCORED and r.D == 1.0                 # (20-5)/(2*5) clipped


def test_engine_is_deterministic():
    sched = steady("c", T0, 60, 5) + steady("c", T0 + 60, 20, 40)
    a, b = run(make(), sched), run(make(), sched)
    assert a == b
