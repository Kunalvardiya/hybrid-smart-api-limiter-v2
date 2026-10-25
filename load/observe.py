"""Verify what a Locust run really did to the system, by reading Redis afterwards.

    python -m load.observe --scenario flash_crowd --run-id r1

Locust only proves requests were SENT. This tool checks that they were recorded as telemetry,
scored by the worker, moved through policy states, and that the scenario's predictions
(`expect` in load/definitions.py) held. It prints a report, writes
load/results/<scenario>-<run_id>-observed.json and exits 1 if a check fails.

Redis connection: --redis-url, else REDIS_HOST/REDIS_PORT/REDIS_PASSWORD from the environment or
.env. For the no-Docker stack (tools/dev_stack.py) use --redis-url redis://127.0.0.1:6390/0
"""
import argparse
import hashlib
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

import redis

from load.definitions import SCENARIOS, STATE_ORDER, identities

RESULTS = Path(__file__).parent / "results"
LEVEL = {v: k for k, v in STATE_ORDER.items()}


def client_id(api_key: str) -> str:
    return "key:" + hashlib.sha256(api_key.encode()).hexdigest()[:16]


def scan(r, key):
    last = "-"
    while True:
        batch = r.xrange(key, min=last, count=2000)
        if not batch:
            return
        yield from batch
        ms, seq = batch[-1][0].split("-")
        last = f"{ms}-{int(seq) + 1}"


def connect(url: str | None):
    if not url:
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
        from app.config import load_settings
        url = load_settings().redis_url
    return redis.Redis.from_url(url, decode_responses=True)


def collect(r, scenario: str, rid: str):
    sc = SCENARIOS[scenario]
    ids = {role: identities(scenario, role, rid) for role in sc.roles}
    cid_to = {client_id(k): (role, k) for role, keys in ids.items() for k in keys}

    telemetry = Counter()
    gaps = 0
    for _i, f in scan(r, "telemetry:events"):
        if f.get("type") == "gap":
            gaps += 1
        elif f.get("client") in cid_to:
            telemetry[f["client"]] += 1

    risk = defaultdict(lambda: {"records": 0, "max_score": 0.0, "max": {"D": 0.0, "B": 0.0, "F": 0.0, "C": 0.0},
                                "statuses": Counter()})
    for _i, f in scan(r, "risk:log"):
        c = f.get("client")
        if c in cid_to:
            s = risk[c]
            s["records"] += 1
            s["statuses"][f.get("status", "?")] += 1
            s["max_score"] = max(s["max_score"], float(f.get("score", 0)))
            for k in "DBFC":
                s["max"][k] = max(s["max"][k], float(f.get(k, 0) or 0))

    transitions = defaultdict(list)
    for _i, f in scan(r, "policy:events"):
        if f["client"] in cid_to:
            transitions[f["client"]].append((f["from"], f["to"], f["reason"]))

    final = {c: r.hgetall(f"policy:{c}") for c in cid_to}
    pending = r.xpending("telemetry:events", "behaviour-workers")["pending"] if r.exists("telemetry:events") else 0
    return ids, cid_to, telemetry, risk, transitions, final, gaps, pending


def max_state(trans) -> str:
    return max(["NORMAL"] + [t[1] for t in trans], key=lambda s: STATE_ORDER[s])


def evaluate(check, value, per_identity):
    """per_identity: list of dicts. Returns (ok_for_each) list of bools."""
    out = []
    for d in per_identity:
        if check == "max_state_at_most":
            out.append(STATE_ORDER[d["max_state"]] <= STATE_ORDER[value])
        elif check == "max_state_at_least":
            out.append(STATE_ORDER[d["max_state"]] >= STATE_ORDER[value])
        elif check == "min_429":
            out.append(d["client"].get("status", {}).get("429", 0) >= value)
        elif check == "max_429":
            out.append(d["client"].get("status", {}).get("429", 0) <= value)
        elif check == "max_transitions":
            out.append(len(d["transitions"]) <= value)
        elif check == "scored":
            out.append(d["risk_records"] > 0 and "score" in d["final"])
        else:
            raise ValueError(check)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    ap.add_argument("--run-id", default=os.environ.get("SCN_RUN_ID", "r1"))
    ap.add_argument("--redis-url")
    ap.add_argument("--client-json", help="default: load/results/<scenario>-<run>-client.json")
    args = ap.parse_args()

    sc = SCENARIOS[args.scenario]
    r = connect(args.redis_url)
    ids, cid_to, telemetry, risk, transitions, final, gaps, pending = collect(r, args.scenario, args.run_id)

    cj = Path(args.client_json or RESULTS / f"{args.scenario}-{args.run_id}-client.json")
    client_side = json.loads(cj.read_text())["clients"] if cj.exists() else {}

    rows, failures = {}, 0
    print(f"\n=== {sc.title}  (scenario={args.scenario}, run_id={args.run_id}) ===")
    print(sc.description)
    if not client_side:
        print(f"!! no client-side counters found at {cj}: request counts and 429 checks are NOT verified")

    for role, keys in ids.items():
        per = []
        for k in keys:
            c = client_id(k)
            tr = transitions[c]
            per.append({"identity": k, "cid": c, "client": client_side.get(k, {}), "telemetry": telemetry[c],
                        "risk_records": risk[c]["records"], "max_score": risk[c]["max_score"],
                        "max_feat": risk[c]["max"], "transitions": tr, "max_state": max_state(tr),
                        "final": final[c], "final_state": final[c].get("state", "NORMAL")})
        rows[role] = per
        sent = sum(d["client"].get("sent", 0) for d in per)
        tel = sum(d["telemetry"] for d in per)
        n429 = sum(d["client"].get("status", {}).get("429", 0) for d in per)
        print(f"\n-- role '{role}' ({len(per)} identities): {sc.roles[role].description}")
        print(f"   requests sent (Locust side) {sent}   telemetry events recorded {tel}   429 responses {n429}")
        print(f"   {'identity':28} {'max state':10} {'final':10} {'scores':>6} {'max score':>9}  max D/B/F/C        transitions")
        for d in per[:12]:
            mf = d["max_feat"]
            print(f"   {d['identity']:28} {d['max_state']:10} {d['final_state']:10} {d['risk_records']:6} "
                  f"{d['max_score']:9.1f}  {mf['D']:.2f}/{mf['B']:.2f}/{mf['F']:.2f}/{mf['C']:.2f}   "
                  f"{' > '.join(t[1] for t in d['transitions']) or '-'}")
        if len(per) > 12:
            print(f"   ... {len(per) - 12} more identities (see JSON)")
        dist = Counter(d["max_state"] for d in per)
        print(f"   highest state reached per identity: {dict(dist)}")

    print("\n-- checks")

    def report(ok, text, soft=False):
        nonlocal failures
        tag = "PASS" if ok else ("WARN" if soft else "FAIL")
        failures += (not ok) and not soft
        print(f"   [{tag}] {text}")

    all_ids = [d for per in rows.values() for d in per]
    sent_total = sum(d["client"].get("sent", 0) for d in all_ids)
    tel_total = sum(d["telemetry"] for d in all_ids)
    if client_side:
        ratio = tel_total / sent_total if sent_total else 0
        report(sent_total > 0 and 0.98 <= ratio <= 1.0001,
               f"telemetry collection: {tel_total} events recorded for {sent_total} requests sent ({ratio:.1%})")
        bypassed = sum(d["client"].get("bypassed", 0) for d in all_ids)
        errors = sum(d["client"].get("errors", 0) for d in all_ids)
        report(bypassed == 0, f"token bucket enforced on every request: {bypassed} requests bypassed it "
               f"(Redis errors made the limiter fail open; a non-zero value invalidates the run)")
        report(errors == 0, f"unexpected responses (5xx / connection errors): {errors}", soft=True)
    else:
        report(tel_total > 0, f"telemetry collection: {tel_total} events recorded (no client counters to compare)")
    report(all(d["risk_records"] > 0 for d in all_ids) if all_ids else False,
           f"behavioural scoring: {sum(d['risk_records'] for d in all_ids)} score records, "
           f"{sum(d['risk_records'] > 0 for d in all_ids)}/{len(all_ids)} identities scored")
    n_tr = sum(len(d["transitions"]) for d in all_ids)
    report(True, f"policy transitions observed: {n_tr} across {sum(bool(d['transitions']) for d in all_ids)} identities"
           + ("  (none: this scenario predicts no restriction)" if n_tr == 0 else ""))
    report(pending == 0, f"stream consumption: {pending} telemetry entries still pending", soft=True)
    report(gaps == 0, f"telemetry loss: {gaps} gap events in the stream (data loss was reported)", soft=True)

    for role, role_def in sc.roles.items():
        for check, value, quant in role_def.expect:
            oks = evaluate(check, value, rows[role])
            ok = all(oks) if quant == "all" else any(oks)
            report(ok, f"prediction [{role}] {check} {value} ({quant}): {sum(oks)}/{len(oks)} identities")

    RESULTS.mkdir(exist_ok=True)
    out = RESULTS / f"{args.scenario}-{args.run_id}-observed.json"
    out.write_text(json.dumps({"scenario": args.scenario, "run_id": args.run_id, "gaps": gaps,
                               "roles": {k: [{**d, "transitions": d["transitions"]} for d in v]
                                         for k, v in rows.items()}}, indent=1, default=str))
    print(f"\nfull observation written to {out}")
    print("RESULT:", "ALL CHECKS PASSED" if not failures else f"{failures} CHECK(S) FAILED")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

