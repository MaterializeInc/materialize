# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Reduce BACKEND_IO completions, not object-store HTTP requests or cache hits.

Write backend-events.jsonl.gz (including reader mappings) and backend-analysis.json.
Reconciliation uses succeeded + failed, not started counters: these events are
emitted after API completion. Strict/loose bounds express scrape and clock
uncertainty, not proof of a one-to-one match. Event emission follows counter
updates, so scheduling or log loss can still prevent reconciliation.
"""

import argparse
import collections
import datetime
import gzip
import json
import re
import statistics
from decimal import Decimal
from pathlib import Path

OPS = ("consensus_cas", "blob_get", "blob_set", "blob_delete")
COUNTERS = ("succeeded_count", "failed_count", "bytes_count")
UUID = r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}"
ANSI = re.compile(r"\x1b\[[0-9;]*m")
NAME = re.compile(r"[\w.-]+(?:::[\w.-]+)*")
FIELD = re.compile(r"([\w.]+)=")


def top_level(text):
    """Yield positions outside Rust debug strings and balanced containers.

    In particular, braces and field-looking text inside quoted purposes are
    data, not span boundaries. Reject truncated diagnostics rather than losing
    a completion silently.
    """
    stack = []
    quoted = escaped = False
    for i, char in enumerate(text):
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
            continue
        if not stack:
            yield i
        if char == '"':
            quoted = True
        elif char in "({[":
            stack.append(char)
        elif char in ")}]":
            if not stack or stack.pop() != {"}": "{", ")": "(", "]": "["}[char]:
                raise ValueError(f"unbalanced diagnostic context: {text}")
    if quoted or stack:
        raise ValueError(f"truncated diagnostic context: {text}")


def value(raw):
    if raw.startswith("Some(") and raw.endswith(")"):
        return value(raw[5:-1])
    if raw == "None":
        return None
    if raw.startswith('"'):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
        # Rust Debug uses \u{...} and \0, unlike JSON. Decode escapes in one
        # pass so a literal escaped backslash never turns into a Unicode escape.
        escapes = {'"': '"', "\\": "\\", "n": "\n", "r": "\r", "t": "\t", "0": "\0"}
        out = []
        i = 1
        while i < len(raw) - 1:
            if raw[i] != "\\":
                out.append(raw[i])
                i += 1
                continue
            i += 1
            if raw[i : i + 2] == "u{":
                end = raw.index("}", i + 2)
                out.append(chr(int(raw[i + 2 : end], 16)))
                i = end + 1
            elif raw[i] == "u":
                out.append(chr(int(raw[i + 1 : i + 5], 16)))
                i += 5
            else:
                out.append(escapes[raw[i]])
                i += 1
        if not raw.endswith('"'):
            raise ValueError(f"invalid quoted field: {raw}")
        return "".join(out)
    if re.fullmatch(r"\d+", raw):
        return int(raw)
    if raw in ("true", "false"):
        return raw == "true"
    return raw


def fields(text):
    starts = []
    for i in top_level(text):
        if i == 0 or text[i - 1].isspace():
            if match := FIELD.match(text, i):
                starts.append(match)
    result = []
    for index, match in enumerate(starts):
        end = starts[index + 1].start() if index + 1 < len(starts) else len(text)
        raw = text[match.end() : end].strip()
        result.append(dict(name=match[1], value=value(raw), raw=raw))
    return result


def spans(context):
    positions = set(top_level(context))
    result = []
    i = 0
    while i < len(context):
        match = NAME.match(context, i)
        if i not in positions or not match:
            i += 1
            continue
        start, i = i, match.end()
        body = ""
        if i < len(context) and context[i] == "{":
            end = i + 1
            while end < len(context) and end not in positions:
                end += 1
            body = context[i + 1 : end - 1]
            i = end
        result.append(dict(name=match[0], fields=fields(body), raw=context[start:i]))
    return result


def unique(candidates):
    values = []
    for candidate in candidates:
        v = candidate["value"]
        if v is not None and v not in values:
            values.append(v)
    return values[0] if len(values) == 1 else None


def key_context(key, op):
    result = dict(key_kind="unknown", key_writer=None, key_version=None, shard=None)
    if key == "LIVENESS":
        result["key_kind"] = "liveness"
    elif op == "consensus_cas" and re.fullmatch("s" + UUID, key):
        result.update(key_kind="consensus", shard=key)
    elif match := re.fullmatch(rf"(s{UUID})/([^/]+)/([pr]{UUID})", key):
        shard, component, leaf = match.groups()
        result["shard"] = shard
        if leaf.startswith("r") and re.fullmatch(r"v\d+", component):
            result["key_kind"] = "rollup"
        elif leaf.startswith("p"):
            if re.fullmatch("w" + UUID, component):
                result.update(key_kind="batch", key_writer=component)
            elif component.startswith("n"):
                result.update(key_kind="batch", key_version=component[1:])
    return result


def parse(line):
    """Parse one diagnostic record. Ignore other logs, fail on malformed records."""
    clean = ANSI.sub("", line).rstrip("\n")
    markers = list(
        re.finditer(
            r"mz_persist_client::backend_audit: (BACKEND_IO|BACKEND_READER)\b", clean
        )
    )
    if not markers:
        return None
    positions = set(top_level(clean))
    marker = next((m for m in markers if m.start() in positions), None)
    if marker is None:
        return None
    header = re.search(r"(\d{4}-\d\d-\d\dT[\d:.]+Z)\s+INFO\s+", clean[: marker.start()])
    if not header:
        raise ValueError(f"missing diagnostic timestamp: {line}")
    process = clean[: header.start()].strip().removesuffix(":")
    role = process or "pubsubd"
    if process.startswith("cluster-"):
        match = re.fullmatch(r"cluster-(.+)-replica-(.+)-gen-(\d+)", process)
        if not match:
            raise ValueError(f"unknown replica prefix: {process}")
        role = f"replica-{match[1]}-{match[2]}"
    context = clean[header.end() : marker.start()]
    event = dict(
        record=marker[1],
        time=datetime.datetime.fromisoformat(header[1]).timestamp(),
        role=role,
        process=process,
        context=context,
        spans=spans(context),
        fields=fields(clean[marker.end() :].strip()),
    )
    candidates = collections.defaultdict(list)
    for source in [*event["spans"], dict(name="event", fields=event["fields"])]:
        for field in source["fields"]:
            candidates[field["name"]].append(dict(source=source["name"], **field))
    event["context_fields"] = dict(candidates)
    required = (
        ("op", "key", "outcome", "bytes")
        if marker[1] == "BACKEND_IO"
        else ("reader", "shard", "purpose")
    )
    for name in required:
        direct = [f for f in event["fields"] if f["name"] == name]
        event[name] = unique(direct)
        if event[name] is None:
            raise ValueError(f"missing or conflicting {name}: {line}")
    if event["record"] == "BACKEND_IO":
        for name in ("bytes", "attempted_bytes", "seqno"):
            event[name] = unique([f for f in event["fields"] if f["name"] == name])
            if event[name] is not None and (
                type(event[name]) is not int or event[name] < 0
            ):
                raise ValueError(f"invalid {name}: {line}")
        event.update(key_context(event["key"], event["op"]))
        if event["shard"]:
            candidates["shard"].append(dict(source="key", value=event["shard"]))
        for name in ("shard", "reader", "writer", "purpose", "command", "updates"):
            event[name] = unique(candidates[name])
        event["context_fields"] = dict(candidates)
    event["conflicts"] = {
        name: rows
        for name, rows in candidates.items()
        if len(
            {
                json.dumps(r["value"], sort_keys=True)
                for r in rows
                if r["value"] is not None
            }
        )
        > 1
    }
    return event


def command_category(event):
    command = event["command"]
    if command == "compare_and_append":
        updates = event["updates"]
        if type(updates) is not int or updates < 0:
            return "append unknown"
        return "append empty" if updates == 0 else "append data"
    return {
        "downgrade_since": "reader maintenance",
        "compare_and_downgrade_since": "reader maintenance",
        "expire_reader": "reader maintenance",
        "expire_writer": "writer maintenance",
        # register is shared by readers and schemas. Do not infer a reader.
        "register": "registration",
        "add_rollup": "rollup",
        "remove_rollups": "gc",
        "merge_res": "compaction",
        "spine_exert": "compaction scheduling",
        "init_state": "initialization",
        "maybe_init_shard": "initialization",
        "become_tombstone": "finalization",
        "upgrade_version": "version maintenance",
        "compare_and_evolve_schema": "schema maintenance",
    }.get(command, "unknown")


def classify(event):
    names = {s["name"] for s in event["spans"]}
    event["command_category"] = command_category(event)
    event["append_kind"] = None
    if "audit_append" in names:
        lower = unique(event["context_fields"].get("lower", []))
        upper = unique(event["context_fields"].get("upper", []))
        if event["updates"] == 0 and lower is not None and upper is not None:
            event["append_kind"] = "heartbeat" if lower == upper else "progress"
        elif type(event["updates"]) is int and event["updates"] > 0:
            event["append_kind"] = "data"
        else:
            event["append_kind"] = "unknown"
    # Leaf spans describe data/implementation, not ownership. A snapshot cursor
    # uses the same prefetch/consolidation implementation as compaction.
    priorities = [
        ("audit_gc", "gc"),
        ("audit_compaction_discard", "compaction cleanup"),
        ("audit_compaction", "compaction"),
        # This marker is exclusively in Compactor's isolated task, unlike the
        # shared prefetch helper used by both compaction and snapshot cursors.
        ("audit_consolidate", "compaction"),
        ("audit_batch_cleanup", "batch cleanup"),
        ("audit_rollup_maintenance", "rollup maintenance"),
        ("audit_snapshot_cursor", "snapshot cursor"),
        ("audit_cursor", "cursor"),
        ("audit_source_fetch", "source fetch"),
        ("audit_row_fetch", "row fetch"),
        ("batch::compact_runs", "builder compaction"),
        ("audit_upload", "upload"),
        ("audit_acquire", "acquisition"),
        ("audit_reader", "reader maintenance"),
        (
            "audit_append",
            event["command_category"] if event["op"] == "consensus_cas" else "append",
        ),
        ("audit_schema_register", "schema registration"),
        ("batch::spill_run", "builder spill"),
        ("audit_part_set", "batch upload"),
        ("audit_rollup_get", "rollup read"),
        ("audit_rollup_set", "rollup write"),
        ("audit_rollup_delete", "rollup deletion"),
    ]
    event["work_candidates"] = [
        dict(span=span, work_class=kind) for span, kind in priorities if span in names
    ]
    event["work_class"] = (
        event["work_candidates"][0]["work_class"]
        if event["work_candidates"]
        else event["command_category"]
    )
    if event["work_class"] == "unknown" and event["key"] == "LIVENESS":
        event["work_class"] = "liveness"
    event["blob_kind"] = event["key_kind"]
    if "audit_part_set" in names:
        event["blob_kind"] = "batch part"
    # Batch-shaped keys can also contain hollow-run indexes. Without a kind
    # span, leave these as batch rather than claiming they are data parts.
    event["owner"] = event["purpose"]
    event["owner_kind"] = "unknown"
    if "purpose" in event["conflicts"] or event["reader_resolution"] == "ambiguous":
        event["owner_kind"] = "conflict"
    elif event["owner"] is not None:
        event["owner_kind"] = "other purpose"
        for prefix in (
            "shard_source(",
            "snapshot ",
            "read txns",
            "commit txns",
            "txn data",
        ):
            if event["owner"].startswith(prefix):
                event["owner_kind"] = prefix.rstrip("( ")
                break


def resolve(records):
    """Join reader IDs across all emitting processes, never by executing role."""
    readers = collections.defaultdict(list)
    for record in records:
        if record["record"] == "BACKEND_READER":
            readers[record["reader"]].append(
                {k: record[k] for k in ("shard", "purpose", "role", "process", "time")}
            )
    events = []
    for event in records:
        if event["record"] != "BACKEND_IO":
            continue
        mappings = readers.get(event["reader"], [])
        matching = [
            m
            for m in mappings
            if event["shard"] is None or m["shard"] == event["shard"]
        ]
        # Critical-reader IDs can be reused across shards. Keep only relevant
        # mappings here, rather than repeating every shard's registration in
        # every event. The registration records remain in the complete ledger.
        event["reader_mappings"] = matching if matching else mappings
        identities = {(m["shard"], m["purpose"]) for m in matching}
        event["reader_resolution"] = (
            "ambiguous"
            if len(identities) > 1 or {"reader", "shard"} & event["conflicts"].keys()
            else (
                "resolved"
                if identities
                else "shard mismatch" if mappings else "unknown"
            )
        )
        purposes = list(event["context_fields"].get("purpose", []))
        purposes.extend(
            dict(source="reader mapping", value=m["purpose"]) for m in matching
        )
        if len({p["value"] for p in purposes if p["value"] is not None}) > 1:
            event["conflicts"]["purpose"] = purposes
        event["purpose"] = unique(purposes)
        if event["reader_resolution"] == "ambiguous":
            event["purpose"] = None
        classify(event)
        events.append(event)
    return events


def totals(events):
    outcomes = collections.Counter(e["outcome"] for e in events)
    known = all(
        e["outcome"]
        in (
            {"committed", "mismatch", "error"}
            if e["op"] == "consensus_cas"
            else {"ok", "missing", "error"} if e["op"] in OPS else set()
        )
        for e in events
    )
    return dict(
        events=len(events),
        outcomes=dict(sorted(outcomes.items())),
        succeeded_count=len(events) - outcomes["error"] if known else None,
        failed_count=outcomes["error"] if known else None,
        bytes_count=sum(e["bytes"] for e in events),
        attempted_bytes=(
            sum(e["attempted_bytes"] for e in events)
            if all(e["attempted_bytes"] is not None for e in events)
            else None
        ),
        attempted_bytes_events=sum(e["attempted_bytes"] is not None for e in events),
    )


def summarize(events):
    # Shards and commands are attribution boundaries. Do not create a group per
    # blob key or handle. Full identities and purposes remain in the ledger.
    dimensions = (
        "role",
        "shard",
        "op",
        "outcome",
        "work_class",
        "command",
        "command_category",
        "append_kind",
        "owner_kind",
        "key_kind",
        "blob_kind",
    )
    grouped = collections.defaultdict(list)
    for event in events:
        grouped[tuple(event[k] for k in dimensions)].append(event)
    return [
        dict(
            zip(dimensions, key),
            **totals(rows),
            example={
                k: rows[0][k]
                for k in (
                    "context",
                    "key",
                    "command",
                    "updates",
                    "reader",
                    "writer",
                    "purpose",
                    "conflicts",
                )
            },
        )
        for key, rows in sorted(grouped.items(), key=lambda item: json.dumps(item[0]))
    ]


def counters(path):
    """Read exact integer counters, retaining absent metrics as absent.

    These metric vectors have only the op label. Reject unexpected label
    layouts and duplicate series rather than quietly dropping or merging them.
    """
    result = {}
    if not path.exists():
        return result
    prefix = "mz_persist_external_"
    pattern = re.compile(
        r'mz_persist_external_(succeeded_count|failed_count|bytes_count)(?:_total)?\{op="([^"\\]+)"\}\s+(\S+)(?:\s+\S+)?\s*'
    )
    with gzip.open(path, "rt") as stream:
        for line in stream:
            if not any(line.startswith(prefix + suffix) for suffix in COUNTERS):
                continue
            match = pattern.fullmatch(line)
            if not match:
                raise ValueError(f"unsupported counter format in {path}: {line}")
            suffix, op, raw = match.groups()
            number = Decimal(raw)
            if (
                not number.is_finite()
                or number < 0
                or number != number.to_integral_value()
            ):
                raise ValueError(f"invalid counter in {path}: {line}")
            if (op, suffix) in result:
                raise ValueError(f"duplicate counter in {path}: {line}")
            result[op, suffix] = int(number)
    return result


def deltas(before, after, op):
    result = {}
    for name in COUNTERS:
        b, a = before.get((op, name)), after.get((op, name))
        delta = a - b if a is not None and b is not None else None
        if delta is not None and delta < 0:
            raise ValueError(f"counter reset: {op}/{name}: {b} -> {a}")
        result[name] = dict(before=b, after=a, delta=delta)
    success, failed = (result[k]["delta"] for k in COUNTERS[:2])
    result["events"] = dict(
        delta=success + failed if success is not None and failed is not None else None
    )
    return result


def reconcile(events, before, after, lo, hi, offset, jitter):
    bounds = {
        "strict": [lo["end"] + offset + jitter, hi["start"] + offset - jitter],
        "loose": [lo["start"] + offset - jitter, hi["end"] + offset + jitter],
        "nominal": [
            (lo["start"] + lo["end"]) / 2 + offset,
            (hi["start"] + hi["end"]) / 2 + offset,
        ],
    }
    selected = {
        name: [e for e in events if start < e["time"] < end]
        for name, (start, end) in bounds.items()
    }
    operations = {}
    for op in sorted(set(OPS) | {e["op"] for e in events}):
        counts = {
            name: totals([e for e in rows if e["op"] == op])
            for name, rows in selected.items()
        }
        checks = deltas(before, after, op)
        for name, check in checks.items():
            strict, loose = counts["strict"][name], counts["loose"][name]
            delta = check["delta"]
            check.update(
                strict=strict,
                loose=loose,
                nominal=counts["nominal"][name],
                reconciled=(
                    strict <= delta <= loose
                    if all(v is not None for v in (strict, delta, loose))
                    else None
                ),
            )
        operations[op] = dict(reconciliation=checks, **counts)
    return (
        dict(
            bounds_wall=bounds,
            elapsed=bounds["nominal"][1] - bounds["nominal"][0],
            operations=operations,
        ),
        selected["nominal"],
    )


def analyze(run):
    records = []
    with (run / "runtime.log").open() as stream:
        for number, line in enumerate(stream, 1):
            try:
                event = parse(line)
            except (ValueError, KeyError, IndexError) as error:
                raise ValueError(f"runtime.log:{number}: {error}") from error
            if event is not None:
                event["line"] = number
                records.append(event)
    events = resolve(records)
    with (run / "usage.jsonl").open() as stream:
        usage = [json.loads(line) for line in stream]
    offsets = [row["wall_time"] - row["time"] for row in usage]
    offset = statistics.median(offsets)
    jitter = max(abs(x - offset) for x in offsets)
    result = dict(
        schema_version=1,
        run=str(run),
        events=len(events),
        reader_records=len(records) - len(events),
        scope="Uncached API completions for consensus_cas, blob_get/set/delete. Not HTTP requests, cache hits, or other consensus/blob operations.",
        clock_offset=offset,
        clock_spread_seconds=jitter,
        all_operations={
            op: totals([e for e in events if e["op"] == op])
            for op in sorted(set(OPS) | {e["op"] for e in events})
        },
        all_groups=summarize(events),
        windows=[],
    )
    boundaries = sorted(
        run.glob("steady-*-metrics-timing.json"),
        key=lambda p: int(p.name.split("-")[1]),
    )
    for before, after in zip(boundaries, boundaries[1:]):
        b, a = json.loads(before.read_text()), json.loads(after.read_text())
        window = dict(before=before.name, after=after.name, roles={})
        selected = []
        for role in sorted(b.keys() | a.keys()):
            if role not in b or role not in a:
                window["roles"][role] = dict(
                    unavailable="missing timing boundary",
                    before=b.get(role),
                    after=a.get(role),
                )
                continue
            bp = run / before.name.replace("metrics-timing.json", f"{role}.prom.gz")
            ap = run / after.name.replace("metrics-timing.json", f"{role}.prom.gz")
            report, nominal = reconcile(
                [e for e in events if e["role"] == role],
                counters(bp),
                counters(ap),
                b[role],
                a[role],
                offset,
                jitter,
            )
            report["metric_files"] = dict(
                before=str(bp),
                after=str(ap),
                before_exists=bp.exists(),
                after_exists=ap.exists(),
            )
            window["roles"][role] = report
            selected.extend(nominal)
        window["groups"] = summarize(selected)
        window["unmeasured_event_roles"] = sorted(
            {e["role"] for e in events} - (b.keys() & a.keys())
        )
        result["windows"].append(window)
    # Write only after reset validation, so a failed reduction cannot publish a
    # new apparently valid report beside an incomplete ledger.
    with gzip.open(run / "backend-events.jsonl.gz", "wt") as stream:
        for record in records:
            stream.write(json.dumps(record) + "\n")
    (run / "backend-analysis.json").write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {"events": len(events), "windows": [w["roles"] for w in result["windows"]]},
            indent=2,
        )
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    analyze(parser.parse_args().run)
