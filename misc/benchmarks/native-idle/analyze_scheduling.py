# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Reduce root-operator elapsed time, without conflating dataflow/operator IDs."""

import json
import sys
from collections import Counter
from pathlib import Path

COLUMNS = ["dataflow_id", "name", "operator_id", "elapsed_ns"]


def roots(snapshot):
    assert snapshot["columns"] == COLUMNS
    result = {}
    for dataflow, name, operator, elapsed in snapshot["rows"]:
        # Synthetic address entries have no scheduling counter. They are not
        # additional scheduled operators or evidence of zero execution time.
        if elapsed is None:
            continue
        key = (str(dataflow), str(operator))
        assert key not in result, ("duplicate root operator", key)
        result[key] = (name, int(elapsed))
    assert result, "no root scheduling counters"
    assert max(Counter(dataflow for dataflow, _ in result).values()) == 1
    return result


def reduce(before, after):
    assert before["finished"] >= before["started"]
    assert after["finished"] >= after["started"]
    elapsed = (
        after["started"] + after["finished"] - before["started"] - before["finished"]
    ) / 2
    assert elapsed > 0
    first, last = roots(before), roots(after)
    rows = []
    for key in first.keys() & last.keys():
        name, start = first[key]
        end_name, end = last[key]
        assert name == end_name and end >= start, (
            "root replaced or counter reset",
            key,
        )
        rows.append(
            {
                "dataflow_id": key[0],
                "operator_id": key[1],
                "name": name,
                "elapsed_seconds_per_second": (end - start) / 1e9 / elapsed,
            }
        )
    assert rows, "no stable root operators"
    rows.sort(key=lambda row: row["elapsed_seconds_per_second"], reverse=True)
    return {
        "elapsed_seconds": elapsed,
        "query_seconds": [
            before["finished"] - before["started"],
            after["finished"] - after["started"],
        ],
        "stable_dataflows": len(rows),
        "added_roots": sorted(last.keys() - first.keys()),
        "removed_roots": sorted(first.keys() - last.keys()),
        "total_scheduling_seconds_per_second": sum(
            row["elapsed_seconds_per_second"] for row in rows
        ),
        "dataflows": rows,
    }


if __name__ == "__main__":
    for argument in sys.argv[1:]:
        run = Path(argument)
        report = reduce(
            json.loads((run / "scheduling-0.json").read_text()),
            json.loads((run / "scheduling-1.json").read_text()),
        )
        (run / "scheduling-analysis.json").write_text(
            json.dumps(report, indent=2) + "\n"
        )
        print(
            run.name,
            report["stable_dataflows"],
            report["total_scheduling_seconds_per_second"],
        )
        for row in report["dataflows"][:12]:
            print(round(row["elapsed_seconds_per_second"], 5), row["name"])
