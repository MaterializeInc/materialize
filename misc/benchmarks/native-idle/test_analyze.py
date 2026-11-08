# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Counter-boundary checks for the recorded experiment's reducer."""

import contextlib
import datetime
import gzip
import io
import json
import tempfile
import unittest
from pathlib import Path

from analyze import analyze, metrics, operation_rates
from analyze_scans import analyze as analyze_scans
from analyze_scans import parse, scan_count, summarize
from analyze_scheduling import COLUMNS, reduce

PREFIX = "mz_persist_external_"


def key(suffix, op="consensus_scan"):
    return (PREFIX + suffix, (("op", op),))


class CounterReduction(unittest.TestCase):
    def test_empty_process_inventory_is_not_zero_load(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory)

            def save(name, value):
                (run / name).write_text(json.dumps(value))

            save(
                "result.json",
                {"status": "passed", "kind": "native", "arguments": {"windows": 1}},
            )
            save("processes.json", [])
            points = []
            for t in range(2):
                points.append(
                    {
                        "time": t,
                        "phase": "steady-1",
                        "processes": [],
                        "available_memory": 1,
                        "postgres_cgroup": {
                            "cpu.stat": "usage_usec 0",
                            "memory.current": "0",
                        },
                    }
                )
                save(f"steady-{t}-metrics-timing.json", {})
                save(f"steady-{t}-memory.json", [])
                save(
                    f"steady-{t}-postgres.json",
                    {
                        "database": [{"stats_reset": "unchanged"}],
                        "wal": [{"stats_reset": "unchanged"}],
                        "tables": [],
                        "table_io": [],
                        "timing": {
                            k: {"end": t}
                            for k in ("database", "wal", "tables", "table_io")
                        },
                    },
                )
            (run / "usage.jsonl").write_text(
                "".join(json.dumps(p) + "\n" for p in points)
            )
            with self.assertRaisesRegex(ValueError, "empty process inventory"):
                analyze(run)

    def test_rust_counter_names_rates_and_completion_latency(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metrics.gz"
            with gzip.open(path, "wt") as stream:
                stream.write(
                    "# TYPE mz_persist_external_started_count counter\n"
                    'mz_persist_external_started_count{op="consensus_scan"} 12\n'
                )
            self.assertEqual(metrics(path), {key("started_count"): 12})
        before = {
            key(name): 0
            for name in (
                "started_count",
                "succeeded_count",
                "failed_count",
                "seconds",
                "bytes_count",
            )
        }
        after = {
            key("started_count"): 4,
            key("succeeded_count"): 3,
            key("failed_count"): 1,
            key("seconds"): 0.4,
            key("bytes_count"): 800,
        }
        coverage = {"added": [], "removed": []}
        rates = operation_rates(before, after, PREFIX, 2, coverage)["consensus_scan"]
        self.assertEqual(rates["started_count"], 2)
        self.assertEqual(rates["bytes_count"], 400)
        self.assertAlmostEqual(rates["mean_latency_ms"], 100)
        self.assertEqual(coverage, {"added": [], "removed": []})

    def test_reset_fails_and_absent_counters_are_not_zero(self):
        coverage = {"added": [], "removed": []}
        with self.assertRaisesRegex(ValueError, "counter reset"):
            operation_rates(
                {key("started_count"): 10},
                {key("started_count"): 9},
                PREFIX,
                2,
                coverage,
            )
        rates = operation_rates(
            {key("started_count"): 10}, {key("bytes_count"): 20}, PREFIX, 2, coverage
        )
        self.assertEqual(rates, {})
        self.assertEqual(coverage["added"][0]["metric"], PREFIX + "bytes_count")
        self.assertEqual(coverage["removed"][0]["metric"], PREFIX + "started_count")


class SchedulingReduction(unittest.TestCase):
    def test_root_identity_and_elapsed_denominator(self):
        before = {
            "columns": COLUMNS,
            "started": 0,
            "finished": 2,
            "rows": [
                [1, "stable", 10, 100],
                [2, "old", 20, 50],
                [1, "stable", 11, None],
            ],
        }
        after = {
            "columns": COLUMNS,
            "started": 10,
            "finished": 12,
            "rows": [
                [1, "stable", 10, 10100],
                [2, "new", 21, 60],
                [1, "stable", 11, None],
            ],
        }
        report = reduce(before, after)
        self.assertEqual(report["stable_dataflows"], 1)
        self.assertAlmostEqual(report["total_scheduling_seconds_per_second"], 1e-6)
        self.assertEqual(report["added_roots"], [("2", "21")])
        self.assertEqual(report["removed_roots"], [("2", "20")])
        after["rows"][0][3] = 99
        with self.assertRaisesRegex(AssertionError, "counter reset"):
            reduce(before, after)


class ScanAttribution(unittest.TestCase):
    @staticmethod
    def event(timestamp, span="audit_rollup_refresh", rows="Some(0)", size="Some(0)"):
        stamp = (
            datetime.datetime.fromtimestamp(timestamp, datetime.UTC)
            .isoformat()
            .replace("+00:00", "Z")
        )
        return (
            f"cluster-s2-replica-s2-gen-0: {stamp} INFO {span}: "
            "mz_persist_client::scan_audit: CONSENSUS_SCAN "
            f'shard="s00000000-0000-0000-0000-000000000001" '
            f"from=SeqNo(7) limit=100 rows={rows} bytes={size}\n"
        )

    @staticmethod
    def counters(path, succeeded, failed=0):
        with gzip.open(path, "wt") as out:
            for suffix, value in [("succeeded", succeeded), ("failed", failed)]:
                out.write(
                    f'mz_persist_external_{suffix}_count{{op="consensus_scan"}} {value}\n'
                )

    def test_empty_failure_and_unclassified_are_distinct(self):
        empty = parse(self.event(1700000000))
        failure = parse(self.event(1700000001, rows="None", size="None"))
        unknown = parse(self.event(1700000002, span="new_caller"))
        self.assertEqual(empty["role"], "replica-s2-s2")
        self.assertEqual(empty["category"], "rollup state refresh")
        self.assertEqual(unknown["category"], "unclassified")
        group = summarize([empty, failure])[0]
        self.assertEqual((group["scans"], group["empty"], group["failed"]), (2, 1, 1))
        self.assertIsNone(parse("an unrelated log event"))

    def test_absent_counter_is_not_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory) / "metrics.gz"
            with gzip.open(p, "wt") as out:
                out.write("some_other_metric 0\n")
            self.assertIsNone(scan_count(p))
            self.counters(p, 0)
            self.assertEqual(scan_count(p), 0)
            self.counters(p, 7, 2)
            self.assertEqual(scan_count(p), 9)

    def test_scrape_uncertainty_missing_events_and_counter_reset(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory)
            epoch = 1700000000
            (run / "usage.jsonl").write_text(
                json.dumps({"time": 1000, "wall_time": epoch + 1000}) + "\n"
            )
            (run / "runtime.log").write_text(
                "".join(self.event(epoch + 1000 + t) for t in [0.05, 1, 9, 10.05])
            )
            for i, start in enumerate([1000, 1010]):
                (run / f"steady-{i}-metrics-timing.json").write_text(
                    json.dumps({"replica-s2-s2": {"start": start, "end": start + 0.1}})
                )
                self.counters(run / f"steady-{i}-replica-s2-s2.prom.gz", 5 + 3 * i)

            def result():
                with contextlib.redirect_stdout(io.StringIO()):
                    analyze_scans(run)
                return json.loads((run / "scan-analysis.json").read_text())["windows"][
                    0
                ]["roles"]["replica-s2-s2"]

            observed = result()
            self.assertEqual(observed["strict_events"], 2)
            self.assertEqual(observed["loose_events"], 4)
            self.assertEqual(observed["metric_delta"], 3)
            self.assertTrue(observed["reconciled"])
            self.counters(run / "steady-1-replica-s2-s2.prom.gz", 10)
            self.assertFalse(result()["reconciled"])
            self.counters(run / "steady-1-replica-s2-s2.prom.gz", 4)
            with self.assertRaisesRegex(ValueError, "counter reset"):
                result()


if __name__ == "__main__":
    unittest.main()
