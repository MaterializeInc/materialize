#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Bulk-fetch traces from Tempo and summarize self-time per span name.

Reads a CSV produced by audit.py (--trace-out), picks N traces per
(padding, scale, op) cell, fetches each from Tempo, and prints a table
of self-time per span name aggregated across the sampled traces.

The point is to see *how a span's cost scales with N* without having to
eyeball individual trace trees: if `apply_updates` self-time grows
linearly with `scale`, that's the smoking gun.

Usage:
    python3 summarize_traces.py audit-tables.csv \\
        [--tempo http://localhost:3200] \\
        [--per-cell 2] \\
        [--top 10]
"""

import argparse
import base64
import csv
import json
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict


def fetch_trace(tempo: str, trace_id: str, tries: int = 5) -> dict | None:
    """Fetch a trace by ID with retry; returns parsed JSON or None."""
    url = f"{tempo}/api/traces/{trace_id}"
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                data = json.loads(r.read())
            if data.get("batches"):
                return data
        except urllib.error.HTTPError as e:
            if e.code != 404:
                print(f"  http {e.code} for {trace_id}", file=sys.stderr)
        except Exception as e:
            print(f"  fetch error {trace_id}: {e}", file=sys.stderr)
        time.sleep(2 + i)
    return None


def parse_spans(trace: dict) -> list[dict]:
    """Extract flat span list with name + duration."""
    spans = []
    for batch in trace.get("batches", []):
        for scope in batch.get("scopeSpans", []):
            for s in scope.get("spans", []):
                start = int(s["startTimeUnixNano"])
                end = int(s["endTimeUnixNano"])
                span_id = base64.b64decode(s.get("spanId", "")).hex()
                parent_id = base64.b64decode(s.get("parentSpanId", "") or "").hex()
                spans.append(
                    {
                        "name": s["name"],
                        "id": span_id,
                        "parent": parent_id,
                        "dur_ns": end - start,
                    }
                )
    return spans


def self_times(spans: list[dict]) -> dict[str, int]:
    """Sum self-time ns per span name across all spans in this trace."""
    children: dict[str, list[dict]] = defaultdict(list)
    by_id = {s["id"]: s for s in spans}
    for s in spans:
        if s["parent"] and s["parent"] in by_id:
            children[s["parent"]].append(s)
    out: dict[str, int] = defaultdict(int)
    for s in spans:
        child_ns = sum(c["dur_ns"] for c in children.get(s["id"], []))
        st = max(0, s["dur_ns"] - child_ns)
        out[s["name"]] += st
    return out


def fmt_us(ns: int) -> str:
    if ns >= 1_000_000:
        return f"{ns/1_000_000:.1f}ms"
    if ns >= 1_000:
        return f"{ns/1_000:.0f}μs"
    return f"{ns}ns"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("csv", help="audit.py --trace-out CSV")
    ap.add_argument("--tempo", default="http://localhost:3200")
    ap.add_argument(
        "--per-cell",
        type=int,
        default=2,
        help="how many trace samples to pull per (padding,scale,op)",
    )
    ap.add_argument(
        "--top",
        type=int,
        default=12,
        help="top N spans by self-time to display per cell",
    )
    ap.add_argument(
        "--min-ms",
        type=float,
        default=0.5,
        help="only show spans with avg self-time >= this many ms",
    )
    args = ap.parse_args()

    # Group rows by (padding, scale, op); pick up to per-cell trace ids each.
    by_cell: dict[tuple[str, int, str], list[str]] = defaultdict(list)
    with open(args.csv) as f:
        for row in csv.DictReader(f):
            if not row["trace_id"]:
                continue
            cell = (row["padding"], int(row["scale"]), row["op"])
            if len(by_cell[cell]) < args.per_cell:
                by_cell[cell].append(row["trace_id"])

    # Fetch traces and aggregate self-time by span name per cell.
    print(f"# fetching traces from {args.tempo}")
    for cell in sorted(by_cell.keys()):
        padding, scale, op = cell
        per_name_ns: dict[str, list[int]] = defaultdict(list)
        for tid in by_cell[cell]:
            tr = fetch_trace(args.tempo, tid)
            if tr is None:
                print(f"  miss {tid} ({padding} scale={scale} {op})", file=sys.stderr)
                continue
            for name, ns in self_times(parse_spans(tr)).items():
                per_name_ns[name].append(ns)
        if not per_name_ns:
            continue

        # Average self-time per span name across the sampled traces.
        avg = sorted(
            [
                (n, sum(v) / len(v))
                for n, v in per_name_ns.items()
                if sum(v) / len(v) / 1_000_000 >= args.min_ms
            ],
            key=lambda x: -x[1],
        )
        print()
        print(
            f"=== padding={padding} scale={scale} op={op} "
            f"(n_traces={len(by_cell[cell])}) ==="
        )
        print(f"{'avg_self':>10}  span")
        for name, ns in avg[: args.top]:
            print(f"{fmt_us(int(ns)):>10}  {name}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
