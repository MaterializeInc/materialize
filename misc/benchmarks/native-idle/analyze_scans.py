# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Attribute actual diagnostic scan events, retaining scrape-boundary uncertainty."""

import argparse
import collections
import datetime
import gzip
import json
import re
import statistics
from pathlib import Path


def classify(context):
    if "write_rollup_for_state" in context:
        return "rollup", "rollup"
    for marker, category in [
        ("audit_rollup_refresh", "rollup state refresh"),
        ("audit_gc_refresh", "GC state refresh"),
        ("audit_schema_refresh", "schema state refresh"),
        ("audit_missing_blob_refresh", "missing-blob diagnosis"),
    ]:
        if marker in context:
            return category, marker
    if "audit_wait_retry" in context:
        m = re.search(r"audit_upper_frontiers\{purpose=([^}]+)\}", context)
        if m:
            return "upper-only fallback", m[1]
        if "audit_writer_upper_wait" in context:
            return "writer-upper fallback", "writer"
        m = re.search(r"purpose=Some\((\".*?\")\) frontier=", context)
        if m:
            purpose = json.loads(m[1])
            return "reader fallback", purpose
        return "other wait fallback", context
    m = re.search(r"audit_command_retry\{command=([^}]+)\}", context)
    if m:
        return "command CAS recovery", m[1]
    if "audit_upper_mismatch" in context:
        return "append upper recovery", "compare_and_append"
    if "audit_explicit_upper" in context:
        return "explicit upper", context.split("audit_explicit_upper")[0]
    if "recent_since" in context:
        return "physical since", "recent_since"
    if "make_machine" in context:
        m = re.search(r"make_machine\{purpose=([^}]+)\}", context)
        return "machine acquisition", m[1] if m else context
    if "fetch_live_diffs_through" in context or "fetch_all_live_states" in context:
        return "live-state maintenance", context
    return "unclassified", context


def parse(line):
    if "CONSENSUS_SCAN " not in line:
        return None
    head, fields = line.split("CONSENSUS_SCAN ", 1)
    m = re.search(r"(\d{4}-\d\d-\d\dT[\d:.]+Z)\s+INFO\s+(.*)", head)
    assert m, line
    prefix = head[: m.start()].strip().removesuffix(":")
    if prefix.startswith("cluster-"):
        p = re.fullmatch(r"cluster-(.+)-replica-(.+)-gen-(\d+)", prefix)
        assert p, prefix
        role = f"replica-{p[1]}-{p[2]}"
    else:
        role = prefix or "pubsubd"
    shard = re.search(r'shard="(s[0-9a-f-]+)"', fields)
    seq = re.search(r"from=SeqNo\((\d+)\)", fields)
    rows = re.search(r"rows=(?:Some\((\d+)\)|(None))", fields)
    size = re.search(r"bytes=(?:Some\((\d+)\)|(None))", fields)
    assert all([shard, seq, rows, size]), line
    context = m[2].removesuffix("mz_persist_client::scan_audit: ")
    category, owner = classify(context)
    return dict(
        time=datetime.datetime.fromisoformat(m[1]).timestamp(),
        role=role,
        shard=shard[1],
        seqno=int(seq[1]),
        rows=int(rows[1]) if rows[1] is not None else None,
        bytes=int(size[1]) if size[1] is not None else None,
        category=category,
        owner=owner,
        context=context,
    )


def summarize(events):
    groups = collections.defaultdict(list)
    for event in events:
        groups[
            (event["role"], event["shard"], event["category"], event["owner"])
        ].append(event)
    return [
        dict(
            role=key[0],
            shard=key[1],
            category=key[2],
            owner=key[3],
            scans=len(rows),
            empty=sum(r["rows"] == 0 for r in rows),
            failed=sum(r["rows"] is None for r in rows),
            rows=sum(r["rows"] or 0 for r in rows),
            bytes=sum(r["bytes"] or 0 for r in rows),
            example=rows[0]["context"],
        )
        for key, rows in sorted(groups.items())
    ]


def scan_count(path):
    with gzip.open(path, "rt") as f:
        metrics = f.read()
    if 'op="consensus_scan"' not in metrics:
        return None

    def value(suffix):
        m = re.search(
            r"^mz_persist_external_"
            + suffix
            + r'(?:_total)?\{op="consensus_scan"\} ([\d.e+]+)$',
            metrics,
            re.M,
        )
        assert m, (path, suffix)
        return int(float(m[1]))

    return value("succeeded_count") + value("failed_count")


def analyze(run):
    with (run / "runtime.log").open() as log:
        events = [event for line in log if (event := parse(line))]
    with gzip.open(run / "scan-events.jsonl.gz", "wt") as out:
        for event in events:
            out.write(json.dumps(event) + "\n")
    result = {
        "run": str(run),
        "events": len(events),
        "windows": [],
        "all_groups": summarize(events),
    }
    with (run / "usage.jsonl").open() as usage_file:
        usage = [json.loads(line) for line in usage_file]
    offsets = [row["wall_time"] - row["time"] for row in usage]
    offset = statistics.median(offsets)
    jitter = max(abs(x - offset) for x in offsets)
    result["clock_offset"] = offset
    result["clock_spread_seconds"] = jitter
    boundaries = sorted(run.glob("steady-*-metrics-timing.json"))
    for before, after in zip(boundaries, boundaries[1:]):
        b, a = json.loads(before.read_text()), json.loads(after.read_text())
        window = {"before": before.name, "after": after.name, "roles": {}}
        selected = []
        for role in b:
            lo, hi = b[role], a[role]
            role_events = [e for e in events if e["role"] == role]
            strict = [
                e
                for e in role_events
                if lo["end"] + offset + jitter
                < e["time"]
                < hi["start"] + offset - jitter
            ]
            loose = [
                e
                for e in role_events
                if lo["start"] + offset - jitter
                < e["time"]
                < hi["end"] + offset + jitter
            ]
            nominal = [
                e
                for e in role_events
                if (lo["start"] + lo["end"]) / 2 + offset
                < e["time"]
                < (hi["start"] + hi["end"]) / 2 + offset
            ]
            selected.extend(nominal)
            bp = run / before.name.replace("metrics-timing.json", f"{role}.prom.gz")
            ap = run / after.name.replace("metrics-timing.json", f"{role}.prom.gz")
            ac, bc = scan_count(ap), scan_count(bp)
            delta = ac - bc if ac is not None and bc is not None else None
            if delta is not None and delta < 0:
                raise ValueError(f"scan counter reset: {bp} -> {ap}")
            window["roles"][role] = dict(
                metric_delta=delta,
                strict_events=len(strict),
                loose_events=len(loose),
                nominal_events=len(nominal),
                reconciled=(
                    len(strict) <= delta <= len(loose) if delta is not None else None
                ),
                elapsed=(hi["start"] + hi["end"] - lo["start"] - lo["end"]) / 2,
                start_wall=(lo["start"] + lo["end"]) / 2 + offset,
                end_wall=(hi["start"] + hi["end"]) / 2 + offset,
            )
        window["groups"] = summarize(selected)
        result["windows"].append(window)
    (run / "scan-analysis.json").write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {"events": len(events), "windows": [w["roles"] for w in result["windows"]]},
            indent=2,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    analyze(parser.parse_args().run)
