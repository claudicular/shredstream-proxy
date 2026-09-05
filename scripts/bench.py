#!/usr/bin/env python3
"""Control an on-demand benchmark and report its local, session-scoped results.

Uses only Python's standard library. Run as the proxy user on the proxy host.
This helper never starts/stops providers, changes networking, or restarts services.
"""

import argparse
import contextlib
import fcntl
import ipaddress
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
import uuid


ADDITIVE = (
    "both_delivered", "contested", "candidate_faster", "baseline_faster", "ties",
    "baseline_only", "candidate_only", "clock_anomalies", "delta_sum_us", "time_saved_sum_us",
)
LIVE_STATES = {"waiting_for_sources", "recording", "draining"}
NAME = re.compile(r"[A-Za-z0-9._:-]{1,80}\Z")


def read_json(path):
    with Path(path).open() as f:
        return json.load(f)


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(value, f, indent=2)
            f.write("\n")
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


@contextlib.contextmanager
def control_lock(path):
    lock = Path(str(path) + ".lock")
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield


def status_path(control):
    return Path(str(control) + ".status.json")


def registry_path(control):
    return Path(str(control) + ".sources.json")


def load_registry(control):
    try:
        return read_json(registry_path(control))
    except FileNotFoundError:
        return {}


def resolve_source(name, registry):
    if not NAME.fullmatch(name):
        raise ValueError("invalid provider name")
    if name in registry:
        return {"name": name, **registry[name]}
    if name in {"jito", "doublezero"}:
        return {"name": name, "logical": name}
    try:
        ip = str(ipaddress.ip_address(name))
        return {"name": name, "ips": [ip]}
    except ValueError as exc:
        raise ValueError(f"unknown provider {name!r}; register it with 'sources set'") from exc


def fresh_status(control):
    try:
        status = read_json(status_path(control))
    except FileNotFoundError as exc:
        raise ValueError("no proxy status: start the proxy with ENABLE_BENCHMARK=true and matching BENCHMARK_CONTROL_PATH") from exc
    age = (time.time_ns() - status.get("updated_at_ns", 0)) / 1e9
    if age > 20 or age < -5:
        raise ValueError(f"proxy status is stale or its clock differs (age={age:.1f}s); verify the process and control path")
    return status


def wait_ack(control, session_id, starting, timeout):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        status = fresh_status(control)
        if status.get("session_id") == session_id:
            states = {"waiting_for_sources", "recording"} if starting else {"draining", "complete", "interrupted"}
            if status.get("state") in states and not status.get("control_error"):
                return status
        if status.get("control_error"):
            # An old error may still be present before the next poll. Wait for
            # acknowledgement rather than treating a write as an applied change.
            last_error = status["control_error"]
        else:
            last_error = None
        time.sleep(0.2)
    raise ValueError(f"command was written but not acknowledged within {timeout}s; run status. Last error: {last_error}")


def start(args):
    with control_lock(args.control):
        current = fresh_status(args.control)
        if current.get("state") in LIVE_STATES:
            raise ValueError("a session is active; stop it and wait for completion first")
        registry = load_registry(args.control)
        baseline = resolve_source(args.baseline, registry)
        candidate = resolve_source(args.candidate, registry)
        if baseline == candidate or baseline["name"] == candidate["name"]:
            raise ValueError("baseline and candidate must differ")
        if not 1 <= args.duration <= 604800 or not 0 <= args.csv_max_rows <= 10000000:
            raise ValueError("duration must be 1..604800 seconds and csv-max-rows 0..10000000")
        session_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:12]
        atomic_json(args.control, {"version": 1, "action": "start", "session": {
            "id": session_id, "baseline": baseline, "candidate": candidate,
            "max_duration_secs": args.duration, "csv_max_rows": args.csv_max_rows,
        }})
        status = wait_ack(args.control, session_id, True, args.timeout)
    print(json.dumps(status, indent=2))


def stop(args):
    with control_lock(args.control):
        current = fresh_status(args.control)
        if current.get("state") not in LIVE_STATES:
            print(json.dumps(current, indent=2))
            return
        session_id = current["session_id"]
        atomic_json(args.control, {"version": 1, "action": "stop", "session_id": session_id})
        status = wait_ack(args.control, session_id, False, args.timeout)
    # Stop ACK means draining, not immediate completion. No need to hold the
    # helper lock while the window ripens.
    if args.wait:
        deadline = time.monotonic() + args.wait
        while status.get("state") == "draining" and time.monotonic() < deadline:
            time.sleep(0.5)
            status = fresh_status(args.control)
            if status.get("session_id") != session_id:
                raise ValueError("another session replaced the status; inspect the stopped session's status.json")
        if status.get("state") == "draining":
            raise ValueError("stop acknowledged; session is still draining. Check status again.")
    print(json.dumps(status, indent=2))


def sources(args):
    with control_lock(args.control):
        registry = load_registry(args.control)
        if args.source_action == "set":
            if not NAME.fullmatch(args.name):
                raise ValueError("invalid provider name")
            if args.logical:
                value = {"logical": args.logical}
            else:
                ips = sorted({str(ipaddress.ip_address(s)) for s in args.ip})
                if "192.0.2.1" in ips or len(ips) > 64:
                    raise ValueError("use logical doublezero for 192.0.2.1; at most 64 provider IPs")
                value = {"ips": ips}
            registry[args.name] = value
            atomic_json(registry_path(args.control), registry)
        print(json.dumps(registry, indent=2))


def aggregate_windows(path, shred_type="data"):
    totals = {}
    identity = None
    with Path(path).open() as f:
        for line_no, line in enumerate(f, 1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid/partial results at line {line_no}; wait for a flush or check output_error") from exc
            row_identity = (row["session"], row["baseline"], row["candidate"])
            if identity is not None and row_identity != identity:
                raise ValueError("results contain mixed session/source identities")
            identity = row_identity
            if row["shred_type"] != shred_type:
                continue
            leader = row["leader"]
            acc = totals.setdefault(leader, dict.fromkeys(ADDITIVE, 0))
            for key in ADDITIVE:
                acc[key] += row[key]
    return identity, totals


def metrics(counts):
    n = counts["contested"]
    both = counts["both_delivered"]
    return {
        **counts,
        "candidate_win_pct": 100 * counts["candidate_faster"] / n if n else None,
        "mean_delta_us": counts["delta_sum_us"] / n if n else None,
        "mean_time_saved_us": counts["time_saved_sum_us"] / n if n else None,
        "candidate_coverage_vs_baseline_pct": 100 * both / (both + counts["baseline_only"]) if both + counts["baseline_only"] else None,
    }


def load_stakes(path):
    obj = read_json(path)
    votes = obj.get("result", obj)
    stakes = {}
    for account in votes.get("current", []) + votes.get("delinquent", []):
        identity = account["nodePubkey"]
        stakes[identity] = stakes.get(identity, 0) + int(account["activatedStake"])
    if not sum(stakes.values()):
        raise ValueError("vote accounts snapshot contains no stake")
    return stakes


def report(args):
    directory = Path(args.session_dir)
    identity, counts = aggregate_windows(directory / "windows.jsonl", args.shred_type)
    manifest = read_json(directory / "manifest.json")
    status = read_json(directory / "status.json")
    result = {
        "session": manifest["session"], "status": status,
        "shred_type": args.shred_type, "min_contested": args.min_contested,
        "global": metrics(counts["ALL"]) if "ALL" in counts else None,
        "validators": [],
    }
    if identity and identity != (manifest["session"]["id"], manifest["session"]["baseline"]["name"], manifest["session"]["candidate"]["name"]):
        raise ValueError("manifest and results have different identities")
    for leader, c in sorted(counts.items()):
        if leader in {"ALL", "unknown"}:
            continue
        result["validators"].append({"leader": leader, "sufficient_samples": c["contested"] >= args.min_contested, **metrics(c)})
    if args.votes:
        stakes = load_stakes(args.votes)
        eligible = [r for r in result["validators"] if r["sufficient_samples"] and r["contested"] > 0 and stakes.get(r["leader"], 0)]
        covered = sum(stakes[r["leader"]] for r in eligible)
        result["stake_weighted"] = {
            "network_stake_covered_pct": 100 * covered / sum(stakes.values()),
            "eligible_validators": len(eligible),
            **{key: sum(stakes[r["leader"]] * r[key] for r in eligible) / covered if covered else None
               for key in ("candidate_win_pct", "mean_delta_us", "mean_time_saved_us")},
        }
    if args.json:
        print(json.dumps(result, indent=2))
        return
    s = manifest["session"]
    print(f"{s['id']}: {s['candidate']['name']} vs {s['baseline']['name']} ({status['state']})")
    print("Delta = candidate - baseline (negative is faster). Savings = benefit of adding candidate on shared shreds.")
    if status.get("state") in LIVE_STATES:
        print("Session is still active: this report contains flushed, finalized windows only.")
    if status.get("output_error") or status.get("observation_drops") or status.get("ts_missing") or status.get("capacity_drops"):
        print("Measurement loss/output error recorded; inspect status before drawing conclusions.")
    if not result["global"] or not result["global"]["contested"]:
        print("No contested samples: relative latency is unavailable.")
    def display(leader, row):
        win = f"{row['candidate_win_pct']:.2f}" if row['candidate_win_pct'] is not None else "N/A"
        delta = f"{row['mean_delta_us']:.2f}" if row['mean_delta_us'] is not None else "N/A"
        saving = f"{row['mean_time_saved_us']:.2f}" if row['mean_time_saved_us'] is not None else "N/A"
        print(f"{leader:44} {row['contested']:10d} {win:>8} {delta:>12} {saving:>12} {row['baseline_only']:10d} {row['candidate_only']:10d}")
    print(f"{'leader':44} {'contested':>10} {'win%':>8} {'delta_us':>12} {'saving_us':>12} {'base_only':>10} {'trial_only':>10}")
    if result["global"]:
        display("ALL", result["global"])
    for row in result["validators"]:
        if row["sufficient_samples"]:
            display(row["leader"], row)
    if "stake_weighted" in result:
        print("Stake-weighted (eligible observed validators only): " + json.dumps(result["stake_weighted"]))


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--control", type=Path, default=Path(os.environ.get("BENCHMARK_CONTROL_PATH", "/var/lib/shredstream/benchmark.json")))
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("start")
    s.add_argument("--baseline", required=True)
    s.add_argument("--candidate", required=True)
    s.add_argument("--duration", type=int, default=86400, help="automatic stop after seconds, including warmup (default 24h; max 7 days)")
    s.add_argument("--csv-max-rows", type=int, default=0, help="optional bounded raw CSV; 0 disables it")
    s.add_argument("--timeout", type=float, default=15)
    s.set_defaults(func=start)
    s = sub.add_parser("stop")
    s.add_argument("--wait", type=float, default=0, help="wait up to this many seconds for draining to finish")
    s.add_argument("--timeout", type=float, default=15)
    s.set_defaults(func=stop)
    s = sub.add_parser("status")
    s.set_defaults(func=lambda a: print(json.dumps(fresh_status(a.control), indent=2)))
    s = sub.add_parser("sources")
    source_sub = s.add_subparsers(dest="source_action", required=True)
    source_sub.add_parser("list").set_defaults(func=sources)
    ss = source_sub.add_parser("set")
    ss.add_argument("name")
    group = ss.add_mutually_exclusive_group(required=True)
    group.add_argument("--ip", action="append")
    group.add_argument("--logical", choices=["jito", "doublezero"])
    ss.set_defaults(func=sources)
    s = sub.add_parser("report")
    s.add_argument("session_dir", type=Path)
    s.add_argument("--min-contested", type=int, default=100)
    s.add_argument("--shred-type", choices=["data", "code"], default="data")
    s.add_argument("--votes", type=Path, help="fresh getVoteAccounts JSON snapshot for stake weighting")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=report)
    return p


def main():
    args = parser().parse_args()
    try:
        args.func(args)
    except (OSError, ValueError, KeyError) as exc:
        print(f"bench: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
