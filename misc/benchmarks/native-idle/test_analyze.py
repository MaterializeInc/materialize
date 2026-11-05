# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Counter-boundary checks for the recorded experiment's reducer."""

import gzip
import tempfile
import unittest
from pathlib import Path

from analyze import metrics, operation_rates
from analyze_scheduling import COLUMNS, reduce

PREFIX = "mz_persist_external_"


def key(suffix, op="consensus_scan"):
    return (PREFIX + suffix, (("op", op),))


class CounterReduction(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
