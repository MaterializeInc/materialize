#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Summarize `run.py` output directories per phase and arm.

Prints the mean hydration time, the speedup against the `columnation` arm, and
the mean peak RSS and swap. A cell's hydration time is its slowest user object.
Peaks come from `mz_internal.mz_cluster_replica_metrics_history`, so run this
before the history's retention drops the windows.
"""

import argparse
import csv
import os
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from run import Mz

GIB = 1024**3


def load(dirs):
    windows, slowest = [], defaultdict(float)
    for d in dirs:
        with open(Path(d) / "results.csv") as f:
            for r in csv.DictReader(f):
                if r["object"].startswith("mz_"):
                    continue
                key = (d, r["phase"], r["arm"], r["trial"])
                secs = float(r["written_s"] or r["hydrated_s"] or 0)
                slowest[key] = max(slowest[key], secs)
        with open(Path(d) / "windows.csv") as f:
            for w in csv.DictReader(f):
                w["slowest_s"] = slowest[(d, w["phase"], w["arm"], w["trial"])]
                windows.append(w)
    return windows


def peaks(mz, windows):
    # The window is extended by one minute to catch the sample after the last
    # write. Cast to numeric before subtracting, both columns are unsigned.
    values = ",".join(
        f"({i}, '{w['replica_id']}', '{w['started_at']}'::timestamptz, "
        f"'{w['ended_at']}'::timestamptz)"
        for i, w in enumerate(windows)
    )
    rows = mz(
        f"WITH w(i, rid, t0, t1) AS (VALUES {values}) "
        "SELECT w.i, max(h.memory_bytes::numeric), "
        "max(h.heap_bytes::numeric - h.memory_bytes::numeric) "
        "FROM w JOIN mz_internal.mz_cluster_replica_metrics_history h "
        "ON h.replica_id = w.rid "
        "AND h.occurred_at BETWEEN w.t0 AND w.t1 + INTERVAL '1 minute' "
        "GROUP BY 1;"
    )
    for row in rows:
        i, rss, swap = row.split("|")
        windows[int(i)]["rss_gib"] = float(rss) / GIB
        windows[int(i)]["swap_gib"] = float(swap) / GIB


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("dirs", nargs="+", help="run.py --outdir directories")
    ap.add_argument("--mz-bin", default="bin/mz")
    ap.add_argument("--config", default=os.environ.get("MZ_CONFIG", ""))
    ap.add_argument("--region", default="aws/us-east-1")
    ap.add_argument("--profile", default="staging")
    ap.add_argument("--no-peaks", action="store_true", help="skip the SQL query")
    args = ap.parse_args()

    windows = load(args.dirs)
    if not args.no_peaks:
        peaks(Mz(args.mz_bin, args.config, args.region, args.profile), windows)

    cells = defaultdict(list)
    for w in windows:
        cells[(w["sf"], w["size"], w["phase"], w["arm"])].append(w)
    print("sf\tsize\tphase\tarm\tn\tmean_s\tspeedup\trss_gib\tswap_gib")
    for (sf, size, phase, arm), ws in sorted(cells.items()):
        mean = statistics.mean(w["slowest_s"] for w in ws)
        base = cells.get((sf, size, phase, "columnation"))
        speedup = statistics.mean(w["slowest_s"] for w in base) / mean if base else 0
        rss = [w["rss_gib"] for w in ws if "rss_gib" in w]
        swap = [w["swap_gib"] for w in ws if "swap_gib" in w]
        print(
            f"{sf}\t{size}\t{phase}\t{arm}\t{len(ws)}\t{mean:.0f}\t{speedup:.2f}\t"
            f"{statistics.mean(rss) if rss else float('nan'):.1f}\t"
            f"{statistics.mean(swap) if swap else float('nan'):.1f}"
        )


if __name__ == "__main__":
    main()
