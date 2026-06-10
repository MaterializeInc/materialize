# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Analyze a Tempo trace JSON file and print a hierarchical span tree."""

import base64
import json
import sys


def decode_id(b64_id):
    """Decode a base64-encoded span/trace ID to hex string."""
    if not b64_id:
        return ""
    try:
        return base64.b64decode(b64_id).hex()
    except Exception:
        return b64_id


def parse_trace(filepath):
    with open(filepath) as f:
        data = json.load(f)
    spans = []
    for batch in data.get("batches", []):
        for scope_spans in batch.get("scopeSpans", []):
            for span in scope_spans.get("spans", []):
                start = int(span["startTimeUnixNano"])
                end = int(span["endTimeUnixNano"])
                attrs = {}
                for a in span.get("attributes", []):
                    v = a.get("value", {})
                    val = (
                        v.get("stringValue")
                        or v.get("intValue")
                        or v.get("boolValue")
                        or v.get("doubleValue")
                        or ""
                    )
                    attrs[a["key"]] = val
                spans.append(
                    {
                        "name": span["name"],
                        "spanId": decode_id(span.get("spanId", "")),
                        "parentSpanId": decode_id(span.get("parentSpanId", "")),
                        "startNs": start,
                        "endNs": end,
                        "durationNs": end - start,
                        "attributes": attrs,
                    }
                )
    return spans


def build_tree(spans):
    by_id = {s["spanId"]: s for s in spans}
    children = {}
    roots = []
    for s in spans:
        pid = s["parentSpanId"]
        if pid and pid in by_id:
            children.setdefault(pid, []).append(s)
        else:
            roots.append(s)
    return roots, children


def fmt_dur(ns):
    ms = ns / 1e6
    if ms >= 1000:
        return f"{ms/1000:.2f}s "
    elif ms >= 1:
        return f"{ms:.1f}ms"
    elif ms >= 0.001:
        return f"{ns/1000:.0f}us"
    else:
        return f"{ns}ns"


def self_time(node, children_map):
    child_time = sum(c["durationNs"] for c in children_map.get(node["spanId"], []))
    return max(0, node["durationNs"] - child_time)


def print_tree(node, children_map, indent=0, min_ms=0.1):
    dur_ms = node["durationNs"] / 1e6
    if dur_ms < min_ms:
        return
    loc = ""
    if "code.file.path" in node["attributes"]:
        loc = f' [{node["attributes"]["code.file.path"]}'
        if "code.line.number" in node["attributes"]:
            loc += f':{node["attributes"]["code.line.number"]}'
        loc += "]"
    st = self_time(node, children_map)
    prefix = "  " * indent
    print(
        f"{prefix}{fmt_dur(node['durationNs']):>10}  (self: {fmt_dur(st):>10})  {node['name']}{loc}"
    )
    for child in sorted(
        children_map.get(node["spanId"], []), key=lambda s: s["startNs"]
    ):
        print_tree(child, children_map, indent + 1, min_ms)


def analyze(filepath, label=""):
    spans = parse_trace(filepath)
    roots, children = build_tree(spans)
    if label:
        print(f"\n{'='*80}\n  {label}\n{'='*80}")
    print(f"\nTotal spans: {len(spans)}")
    print(f"Root duration: {fmt_dur(sum(r['durationNs'] for r in roots))}")

    # Top by self-time
    ranked = sorted(
        [(s, self_time(s, children)) for s in spans], key=lambda x: x[1], reverse=True
    )
    print("\nTop 15 spans by self-time (where actual work happens):")
    print(f"{'Self Time':>12}  {'Total':>12}  Name")
    print(f"{'-'*12}  {'-'*12}  {'-'*60}")
    for s, st in ranked[:15]:
        print(f"{fmt_dur(st):>12}  {fmt_dur(s['durationNs']):>12}  {s['name']}")

    # Span tree
    print(f"\nSpan tree (>= 0.1ms):\n{'-'*80}")
    for root in sorted(roots, key=lambda s: s["startNs"]):
        print_tree(root, children)
        print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <trace.json> [label]")
        sys.exit(1)
    analyze(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "")
