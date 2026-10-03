# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Summarize fio JSON outputs: bandwidth, IOPS, and completion-latency percentiles."""

import json
import pathlib
import sys

rows = []
for p in sorted(pathlib.Path(sys.argv[1]).glob("*.json")):
    try:
        data = json.loads(p.read_text())
    except json.JSONDecodeError:
        continue
    for job in data.get("jobs", []):
        for kind in ("read", "write"):
            d = job[kind]
            if d["io_bytes"] == 0:
                continue
            pct = d["clat_ns"].get("percentile", {})
            rows.append(
                (
                    (
                        p.stem
                        if job["jobname"] == p.stem
                        else f"{p.stem}/{job['jobname']}"
                    ),
                    kind,
                    d["bw_bytes"] / 2**30,
                    d["iops"],
                    pct.get("50.000000", 0) / 1e3,
                    pct.get("99.000000", 0) / 1e3,
                    job.get("usr_cpu", 0) + job.get("sys_cpu", 0),
                )
            )
print(
    f"{'run':52} {'op':5} {'GiB/s':>7} {'IOPS':>9} {'p50us':>8} {'p99us':>9} {'cpu%':>6}"
)
for r in rows:
    print(
        f"{r[0]:52} {r[1]:5} {r[2]:7.2f} {r[3]:9.0f} {r[4]:8.0f} {r[5]:9.0f} {r[6]:6.1f}"
    )
