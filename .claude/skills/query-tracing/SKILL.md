---
name: query-tracing
description: >
  This skill should be used when the user wants to debug or trace where time is
  spent during query execution in Materialize. Trigger when the user mentions
  tracing queries, span analysis, query latency breakdown, Tempo traces,
  trace IDs, or wants to understand where time goes in CREATE TABLE, SELECT,
  INSERT, DROP, or any other SQL statement execution.
---

# Query Tracing Skill

Use distributed tracing (OpenTelemetry / Tempo) to understand where time is
spent during SQL statement execution in a local Materialize instance.

## Important: Use Optimized Builds

**Always trace `--optimized` builds.** Debug builds introduce significant
artifacts — unoptimized code, extra debug checks, and inflated self-times that
don't reflect production behavior. Tracing a debug build can be misleading
because time may appear in places that are negligible in release builds.

```bash
bin/environmentd --optimized --monitoring
```

The `--optimized` build takes longer to compile but produces traces that
accurately represent where time is actually spent.

## Prerequisites

The local monitoring stack (Tempo + Grafana + Prometheus) and `environmentd`
with tracing enabled must be running. All network calls require
`dangerouslyDisableSandbox`.

**Important: Start the monitoring stack _before_ environmentd.** If Tempo is
not running when environmentd starts sending spans, those spans are lost —
they will not be retroactively captured.

### 1. Start the monitoring stack

```bash
cd misc/monitoring && ./mzcompose run default
```

Wait for Tempo to be ready:

```bash
# Poll until Tempo is accepting requests
for i in $(seq 1 30); do
    curl -s -o /dev/null -w "%{http_code}" http://localhost:3200/ready | grep -q 200 && break
    sleep 5
done
```

### 2. Start environmentd with tracing

```bash
bin/environmentd --optimized --monitoring
```

Or with the trace filter pre-set:

```bash
bin/environmentd --optimized --reset --monitoring -- --system-parameter-default='opentelemetry_filter=debug'
```

### 3. Verify both services are running

Before tracing, confirm the stack is healthy:

```bash
# Tempo accepting traces?
curl -s -o /dev/null -w "%{http_code}" http://localhost:3200/ready
# environmentd listening?
lsof -tiTCP:6875 -sTCP:LISTEN
```

### 4. Set the trace filter at runtime (if not set at startup)

Connect as `mz_system` and set the OpenTelemetry filter:

```bash
psql -U mz_system -h localhost -p 6877 materialize -c "ALTER SYSTEM SET opentelemetry_filter = 'debug';"
```

`debug` is a good starting filter. For even more detail, use `trace` (but
expect much more data). You can also target specific modules:

```
ALTER SYSTEM SET opentelemetry_filter = 'mz_adapter::coord=trace,debug';
```

## Workflow: Trace a Query

### Step 1: Run the query and capture the trace ID

Use `psql` with `emit_trace_id_notice` enabled. **Important:** Use separate
`-c` flags for SET and the query, since DDL statements (CREATE, DROP, ALTER)
cannot run inside an implicit transaction block:

```bash
psql -U materialize -h localhost -p 6875 materialize \
  -c "SET emit_trace_id_notice = true;" \
  -c "<YOUR SQL STATEMENT HERE>;"
```

The output will contain NOTICE lines with trace IDs — one per statement. The
SET statement gets its own trace ID; the second trace ID is the one you want:

```
SET
NOTICE:  trace id: fd2f69eb059e4823d8d48a87ccab8f6f
NOTICE:  trace id: 984bd8c69f99703243d350e14b02caea
CREATE TABLE
```

The trace ID for the query is the **last** one before the statement result.

To extract the trace ID programmatically:

```bash
OUTPUT=$(psql -U materialize -h localhost -p 6875 materialize \
  -c "SET emit_trace_id_notice = true;" \
  -c "CREATE TABLE foo (id INT);" 2>&1)
TRACE_ID=$(echo "$OUTPUT" | grep "trace id:" | tail -1 | sed 's/.*trace id: //')
echo "Trace ID: $TRACE_ID"
```

### Step 2: Wait for spans to be flushed

Spans are batched before export (default 5s delay, configured in
`--opentelemetry-sched-delay`). Wait ~10 seconds after the query completes.

### Step 3: Fetch the trace from Tempo

Query the Tempo HTTP API on port 3200. Retry if needed — spans may still be
in flight:

```bash
sleep 10
# Retry loop — first attempt often gets "trace not found"
for i in $(seq 1 5); do
    RESP=$(curl -s http://localhost:3200/api/traces/$TRACE_ID)
    if echo "$RESP" | grep -q "batches"; then
        echo "$RESP" > /tmp/claude-1000/trace.json
        echo "Trace fetched ($(echo "$RESP" | wc -c) bytes)"
        break
    fi
    echo "Trace not ready, retrying... ($i/5)"
    sleep 5
done
```

### Step 4: Analyze the trace

The Tempo API returns traces in OTLP JSON format. **Important:** Span IDs and
parent span IDs are base64-encoded, not hex. The Python script below handles
this.

Save this as `/tmp/claude-1000/trace_tree.py`:

```python
#!/usr/bin/env python3
"""Analyze a Tempo trace JSON file and print a hierarchical span tree."""

import json, sys, base64


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
                    val = (v.get("stringValue") or v.get("intValue")
                           or v.get("boolValue") or v.get("doubleValue") or "")
                    attrs[a["key"]] = val
                spans.append({
                    "name": span["name"],
                    "spanId": decode_id(span.get("spanId", "")),
                    "parentSpanId": decode_id(span.get("parentSpanId", "")),
                    "startNs": start, "endNs": end,
                    "durationNs": end - start,
                    "attributes": attrs,
                })
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
    if ms >= 1000: return f"{ms/1000:.2f}s "
    elif ms >= 1:  return f"{ms:.1f}ms"
    elif ms >= 0.001: return f"{ns/1000:.0f}us"
    else: return f"{ns}ns"


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
    print(f"{prefix}{fmt_dur(node['durationNs']):>10}  (self: {fmt_dur(st):>10})  {node['name']}{loc}")
    for child in sorted(children_map.get(node["spanId"], []),
                        key=lambda s: s["startNs"]):
        print_tree(child, children_map, indent + 1, min_ms)


def analyze(filepath, label=""):
    spans = parse_trace(filepath)
    roots, children = build_tree(spans)
    if label:
        print(f"\n{'='*80}\n  {label}\n{'='*80}")
    print(f"\nTotal spans: {len(spans)}")
    print(f"Root duration: {fmt_dur(sum(r['durationNs'] for r in roots))}")

    # Top by self-time
    ranked = sorted([(s, self_time(s, children)) for s in spans],
                    key=lambda x: x[1], reverse=True)
    print(f"\nTop 15 spans by self-time (where actual work happens):")
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
```

Run:

```bash
python3 /tmp/claude-1000/trace_tree.py /tmp/claude-1000/trace.json "My Query"
```

## Interpreting Results

- **Self-time** is where actual work happens. A span with 100ms duration but
  0ms self-time is just a wrapper — its children do all the work.
- **`group_commit_apply::append_fut`** — Persist append operations, often
  dominant for DDL. Time here is spent writing to the durable log.
- **`catalog::transact_inner`** — Catalog transaction processing. High
  self-time means catalog state manipulation.
- **`oracle::write_ts` / `oracle::apply_write`** — Timestamp oracle calls to
  CockroachDB. Each is a round-trip.
- **`consensus::compare_and_set` / `consensus::scan`** — Persist consensus
  operations (also CockroachDB round-trips).
- **`coord::check_consistency`** — Post-DDL consistency check. Self-time here
  is pure computation.
- **Sequential vs. concurrent children** — children with overlapping time
  ranges are concurrent; non-overlapping are sequential.
- **Code locations** in `[src/...]` brackets let you jump directly to the
  source code.

## Tips

- **Use `--optimized` builds.** Debug builds inflate self-times with
  unoptimized code paths and extra debug checks (e.g., `check_consistency`
  may appear much larger than in production). Always trace optimized builds
  for meaningful results.
- **Start monitoring before environmentd.** Spans emitted before Tempo is
  ready are silently dropped. If you restart environmentd, make sure Tempo
  is still running.
- If traces are empty or incomplete, the `opentelemetry_filter` may be too
  restrictive. Try `debug` or even `trace`.
- The first fetch after a query often returns `trace not found`. The default
  batch delay is 5s, but ingestion adds latency. Always use a retry loop.
- Tempo retains traces for only 15 minutes by default (configured in
  `misc/monitoring/tempo.yml`). Fetch traces promptly.
- To search for recent traces: `curl -s "http://localhost:3200/api/search?limit=10"`
- You can also view traces in Grafana at http://localhost:3000 by searching
  for the trace ID in the Tempo datasource explore view.
- For comparing before/after a code change, trace the same query on both
  builds and compare the self-time rankings side by side.
