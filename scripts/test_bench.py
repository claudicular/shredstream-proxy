"""Offline tests for benchmark control and reporting. No network or service changes."""

import argparse
import contextlib
import io
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import bench


def row(leader="ALL", **values):
    return {"session": "test", "baseline": "A", "candidate": "B", "leader": leader,
            "shred_type": "data", **dict.fromkeys(bench.ADDITIVE, 0), **values}


class BenchTests(unittest.TestCase):
    def test_counts_and_savings_aggregate_without_double_counting_rollup(self):
        with tempfile.TemporaryDirectory(prefix="ss-bench-report-") as d:
            p = Path(d) / "windows.jsonl"
            rows = [row(k, contested=2, both_delivered=2, candidate_faster=1,
                        ties=1, delta_sum_us=-500, time_saved_sum_us=500)
                    for k in ("ALL", "validator")]
            rows += [row(k, contested=1, both_delivered=1, baseline_faster=1,
                         baseline_only=2, delta_sum_us=100, time_saved_sum_us=0)
                     for k in ("ALL", "validator")]
            p.write_text("".join(json.dumps(r) + "\n" for r in rows))
            identity, totals = bench.aggregate_windows(p)
            self.assertEqual(identity, ("test", "A", "B"))
            self.assertEqual(totals["ALL"]["contested"], 3)
            self.assertEqual(totals["validator"], totals["ALL"])
            m = bench.metrics(totals["ALL"])
            self.assertAlmostEqual(m["candidate_win_pct"], 100 / 3)
            self.assertAlmostEqual(m["mean_delta_us"], -400 / 3)
            self.assertAlmostEqual(m["mean_time_saved_us"], 500 / 3)
            self.assertEqual(m["candidate_coverage_vs_baseline_pct"], 60)

    def test_no_comparison_is_unavailable_not_zero_latency(self):
        m = bench.metrics(row(candidate_only=100))
        self.assertIsNone(m["mean_delta_us"])
        self.assertIsNone(m["candidate_win_pct"])
        self.assertIsNone(m["candidate_coverage_vs_baseline_pct"])

    def test_mixed_sessions_and_partial_rows_are_rejected(self):
        with tempfile.TemporaryDirectory(prefix="ss-bench-invalid-") as d:
            p = Path(d) / "windows.jsonl"
            a, b = row(), row(session="different")
            p.write_text(json.dumps(a) + "\n" + json.dumps(b) + "\n")
            with self.assertRaisesRegex(ValueError, "mixed"):
                bench.aggregate_windows(p)
            p.write_text(json.dumps(a) + "\n{")
            with self.assertRaisesRegex(ValueError, "partial"):
                bench.aggregate_windows(p)

    def test_sources_snapshot_and_atomic_updates(self):
        with tempfile.TemporaryDirectory(prefix="ss-bench-source-") as d:
            p = Path(d) / "control.json"
            args = argparse.Namespace(control=p, source_action="set", name="provider",
                                      logical=None, ip=["10.0.0.1", "10.0.0.2", "10.0.0.1"])
            with contextlib.redirect_stdout(io.StringIO()):
                bench.sources(args)
            source = bench.resolve_source("provider", bench.load_registry(p))
            self.assertEqual(source["ips"], ["10.0.0.1", "10.0.0.2"])
            self.assertFalse(p.exists())  # registration does not activate recording
            self.assertEqual(bench.resolve_source("doublezero", {})["logical"], "doublezero")
            with self.assertRaises(ValueError):
                bench.resolve_source("missing", {})

    def test_start_requires_live_proxy_and_acknowledgement(self):
        with tempfile.TemporaryDirectory(prefix="ss-bench-control-") as d:
            p = Path(d) / "control.json"
            with self.assertRaisesRegex(ValueError, "no proxy status"):
                bench.fresh_status(p)
            bench.atomic_json(bench.status_path(p), {"updated_at_ns": 1, "state": "idle"})
            with self.assertRaisesRegex(ValueError, "stale"):
                bench.fresh_status(p)
            bench.atomic_json(bench.status_path(p), {"updated_at_ns": time.time_ns(), "state": "idle"})
            with self.assertRaisesRegex(ValueError, "not acknowledged"):
                bench.wait_ack(p, "not-active", True, 0)
            bench.atomic_json(bench.status_path(p), {"updated_at_ns": time.time_ns(),
                "state": "waiting_for_sources", "session_id": "active", "control_error": None})
            self.assertEqual(bench.wait_ack(p, "active", True, 1)["session_id"], "active")

    def test_stake_report_uses_per_validator_rates_and_reports_covered_stake(self):
        with tempfile.TemporaryDirectory(prefix="ss-bench-stake-") as d:
            p = Path(d)
            bench.atomic_json(p / "manifest.json", {"session": {"id": "test", "baseline": {"name": "A"}, "candidate": {"name": "B"}}})
            bench.atomic_json(p / "status.json", {"state": "complete"})
            bench.atomic_json(p / "votes.json", {"result": {"current": [
                {"nodePubkey": "v1", "activatedStake": 10},
                {"nodePubkey": "v2", "activatedStake": 30},
                {"nodePubkey": "unobserved", "activatedStake": 60}], "delinquent": []}})
            rows = [row("v1", contested=100, both_delivered=100, candidate_faster=100, delta_sum_us=-1000, time_saved_sum_us=1000),
                    row("v2", contested=1000, both_delivered=1000, baseline_faster=1000, delta_sum_us=1000)]
            (p / "windows.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
            args = argparse.Namespace(session_dir=p, shred_type="data", min_contested=100, votes=p / "votes.json", json=True)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                bench.report(args)
            sw = json.loads(out.getvalue())["stake_weighted"]
            self.assertEqual(sw["network_stake_covered_pct"], 40)
            self.assertEqual(sw["candidate_win_pct"], 25)
            self.assertEqual(sw["mean_time_saved_us"], 2.5)


if __name__ == "__main__":
    unittest.main()
