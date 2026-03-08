# Consensus Service — Benchmarks & Spec Sheet

**Always run with `--release`.** Debug mode adds significant overhead that
distorts results, especially at high shard counts or with gRPC.

## Micro-benchmarks (criterion)

Fixed micro-scenarios for regression detection. Criterion benchmarks live in
`benches/` and are always compiled in release mode by cargo.

```bash
cargo bench -p mz-persist-consensus-svc                     # all benchmarks
cargo bench -p mz-persist-consensus-svc -- cas_single_shard  # by name
cargo bench -p mz-persist-consensus-svc -- --list            # list available
```

Benchmarks are split into **plumbing** (direct actor channel) and **porcelain**
(full gRPC stack).

## Spec sheet

A configurable workload simulator that answers questions like *"how does the
service behave with 10,000 shards, 2 concurrent writers each, 4KiB values,
under S3 Express latency?"* — and reports latency percentiles, throughput,
flush efficiency, and memory usage.

### Quick start

```bash
# Smoke test — 100 shards, 5 second run
cargo run --release -p mz-persist-consensus-svc --example specsheet -- \
    --num-shards 100 --writers-per-shard 1 --duration 5 --warmup 2

# From a YAML scenario file
cargo run --release -p mz-persist-consensus-svc --example specsheet -- \
    --workload examples/scenarios/small-smoke.yaml

# Full gRPC stack (measures serialization + HTTP/2 overhead)
cargo run --release -p mz-persist-consensus-svc --example specsheet -- \
    --workload examples/scenarios/small-smoke.yaml --transport grpc

# YAML scenario with CLI overrides
cargo run --release -p mz-persist-consensus-svc --example specsheet -- \
    --workload examples/scenarios/production-10k.yaml --duration 30

# JSON output for scripting
cargo run --release -p mz-persist-consensus-svc --example specsheet -- \
    --workload examples/scenarios/small-smoke.yaml --json | jq '.throughput'
```

### Workload model

The simulator spawns `num_shards × writers_per_shard` client tasks. Each client
is bound to a single shard and does the full operation mix at the configured
rate. Every cycle, the client rolls an operation based on the percentage
breakdown:

- **CAS** (default 90%) — compare-and-set with the client's tracked seqno. On
  rejection (another writer won), the client reads head to re-sync and retries
  immediately. CAS blocks until the actor flushes, matching production persist
  client behavior.
- **Scan** (default 8%) — scan entries from the shard.
- **Head** (default 1%) — read the latest entry.
- **Truncate** (default 1%) — if the shard has more than `truncate_to` entries,
  truncate the oldest ones.

The per-client operation rate is derived from `write_rate_per_second` and
`cas_pct`: if you want 1 CAS/s and CAS is 90% of ops, each client does
1/0.9 ≈ 1.11 total ops/s.

A configurable warmup period excludes startup transients from measurements.
Each client records latencies in a local `Vec<Duration>` (no contention);
percentiles are computed by sorting after the run.

### Transport modes

Use `--transport` to select how clients talk to the actor:

- **`direct`** (default) — clients send commands over the actor's `mpsc`
  channel. Measures actor performance in isolation.
- **`grpc`** — a tonic gRPC server starts on a loopback port and all clients go
  through `ConsensusServiceClient`. Measures the full stack: proto serialization,
  HTTP/2 framing, tonic dispatch, and the actor.

Comparing the two directly quantifies gRPC overhead.

### WAL latency profiles

The actor's WAL writer is replaced with a `LatencyWalWriter` that simulates
storage latency without touching S3. Use `--wal-latency` to select a profile:

| Profile | Behavior |
|---------|----------|
| `zero` | Instant flushes. Isolates actor CPU cost. |
| `fixed-5ms` | Every flush takes exactly 5ms. |
| `fixed-10ms` | Every flush takes exactly 10ms. |
| `s3-express` | 95% of flushes take 5ms, 5% take 50ms. Models S3 Express One Zone. |
| `s3-standard` | 95% of flushes take 20ms, 5% take 200ms. Models S3 Standard. |

### Configuration

Every parameter is available as both a CLI flag and a YAML field. CLI flags
override YAML values when both are specified.

| Parameter | CLI flag | Default |
|-----------|----------|---------|
| Shard count | `--num-shards` | 1000 |
| Writers/shard | `--writers-per-shard` | 2 |
| Value size (bytes) | `--value-size` | 64 |
| Max entries | `--max-entries` | 1000 |
| CAS % | `--cas-pct` | 90 |
| Scan % | `--scan-pct` | 8 |
| Head % | `--head-pct` | 1 |
| Truncate % | `--truncate-pct` | 1 |
| Scan limit | `--scan-limit` | 10 |
| Truncate to | `--truncate-to` | 500 |
| Write rate (CAS ops/s/client) | `--write-rate-per-second` | 1.0 |
| Flush interval (ms) | `--flush-interval-ms` | 5 |
| WAL latency profile | `--wal-latency` | `zero` |
| Transport | `--transport` | `direct` |
| Duration (s) | `--duration` | 60 |
| Warmup (s) | `--warmup` | 10 |
| JSON output | `--json` | false |

Operation mix percentages must sum to 100.

### YAML scenario files

The YAML format is flat — field names match CLI flags exactly (minus `--`).
Any field can be omitted to use the default.

```yaml
name: "production-10k"
description: "10K shards with 2 concurrent writers, 4KiB inline values"

num_shards: 10000
writers_per_shard: 2
value_size: 4096
max_entries: 1000

cas_pct: 90
scan_pct: 8
head_pct: 1
truncate_pct: 1

scan_limit: 10
truncate_to: 500

write_rate_per_second: 1.0
flush_interval_ms: 5

wal_latency: "s3-express"

duration: 60
warmup: 10
```

Ready-to-use scenarios in `scenarios/`:

- **`small-smoke.yaml`** — 100 shards, zero WAL latency, 7 second run. Quick
  sanity check.
- **`production-10k.yaml`** — 10K shards, 2 writers each, 4KiB values,
  s3-express latency. Models a realistic production deployment.
- **`stress-large-values.yaml`** — 1K shards, 4KiB values, high write rate
  (5 CAS/s/client). Stress tests memory and flush throughput.
- **`scale-out-no-contention.yaml`** — 10K shards, 1 writer each, s3-express
  latency. Isolates scaling behavior without CAS contention.

### Reading the output

```
=== Consensus Service Spec Sheet ===
Scenario: default (100 shards, 2 writers/shard, 64 B values, 200 target writes/s)
Transport: direct, WAL latency: zero
Duration: 5s (2s warmup), flush interval: 5ms
Op mix: 90% CAS / 8% scan / 1% head / 1% truncate

--- Latencies (ms) ---
  Operation             count      p50      p90      p95      p99      max
  CAS committed           825     4.67     5.84     5.87    11.35    11.39
  Head                     16     0.02     0.04     0.04     0.04     0.04
  Scan                    138     0.02     0.05     0.13     0.23     0.55
  CAS rejected            681     2.39     4.77     5.87     5.87     6.57

--- Throughput ---
  ops/s
    CAS                     301
      committed             165
      rejected              136
    Head                      3
    Scan                     27
    Truncate                  0
    Total (actor)           332
    Total (client)          195  (est. 222)

  Flushes/sec             7.0  (every 142.9ms)
  Ops/flush              28.0
  Bytes/flush         3.2 KiB

--- Clients (200) ---
  ops/s per client        min       p50       p90       max
  Actual                  1.1       1.1       1.1       1.1
  Estimated               1.1

--- Actor State (includes warmup) ---
  Shards:              100
  Entries:           1,330
  Memory:         83.1 KiB
```

**Latencies** — per-operation percentiles in milliseconds. Operations with
zero samples are omitted. CAS committed and rejected are tracked separately;
rejected CAS latency measures only the failed CAS call (the subsequent head
re-sync is not included).

**Throughput** — per-operation rates and two totals:
- **Total (actor)** — all operations the actor processed, including CAS
  rejections. This is the real load on the service.
- **Total (client)** — successful operations visible to clients (rejections
  are invisible — a rejected CAS followed by a successful retry just looks
  like one CAS from the client's perspective). The parenthetical `est.` is
  the rate you configured.

Low ops/flush means the collection window is too short relative to the op
rate. Large bytes/flush with high flush latency suggests the WAL write is
the bottleneck.

**Clients** — distribution of per-client throughput (min/p50/p90/max). If
the spread is wide, some shards are getting starved. If actual is well below
estimated, the actor can't keep up (compare `--transport direct` vs `grpc`
to isolate gRPC overhead).

**Actor State** — in-memory footprint at end of run. Includes entries written
during warmup, so entry counts will be higher than latency sample counts.

### Parameter sweeps

The `--json` flag and CLI overrides make it easy to sweep a single parameter
across a base scenario. The pattern:

1. Pick a base YAML scenario
2. Loop over parameter values, passing `--json` and the override flag
3. Extract metrics with `jq`

#### Example: value size sweep

`scenarios/sweep-value-size.sh` sweeps value sizes from 1B to 64KiB against
the `scale-out-no-contention` scenario:

```bash
./examples/scenarios/sweep-value-size.sh
./examples/scenarios/sweep-value-size.sh --duration 15 --warmup 3   # shorter run
```

Sample output:

```
val_size        CAS/s    CAS p50    CAS p99  flushes/s  ops/flush bytes/flush       memory
1 B             48760    19.94 ms    66.63 ms       48.9     1096.2     27.1 KiB    863.6 KiB
64 B            49217    19.50 ms    74.85 ms       51.3     1055.8     97.3 KiB     54.4 MiB
256 B           49215    17.65 ms    70.13 ms       24.9     2170.7    653.8 KiB    217.6 MiB
1 KiB           49232    33.91 ms    81.73 ms       34.3     1577.5      1.7 MiB    870.6 MiB
4 KiB           49402    40.22 ms    87.07 ms       34.5     1576.1      6.8 MiB      3.4 GiB
16 KiB          48613   112.67 ms   326.74 ms       44.4     1203.4     20.5 MiB     13.3 GiB
64 KiB          15411   638.09 ms  1719.99 ms        4.7     3619.3    312.1 MiB     21.3 GiB
```

#### Writing your own sweep

Use the same pattern for any parameter. For example, sweeping shard count:

```bash
for shards in 100 1000 5000 10000 50000; do
    cargo run --release -p mz-persist-consensus-svc --example specsheet -- \
        --workload examples/scenarios/scale-out-no-contention.yaml \
        --num-shards "$shards" --json 2>/dev/null \
    | jq -r "[\"$shards\", .throughput.ops_per_sec,
              .latencies[\"CAS committed\"].p50_ms,
              .latencies[\"CAS committed\"].p99_ms] | @tsv"
done | column -t
```

Or sweeping WAL latency profiles:

```bash
for profile in zero fixed-5ms s3-express s3-standard; do
    cargo run --release -p mz-persist-consensus-svc --example specsheet -- \
        --workload examples/scenarios/scale-out-no-contention.yaml \
        --wal-latency "$profile" --json 2>/dev/null \
    | jq -r "[\"$profile\", .throughput.ops_per_sec,
              .latencies[\"CAS committed\"].p50_ms,
              .throughput.flushes_per_sec] | @tsv"
done | column -t
```

Tips:
- Use `--duration 15 --warmup 3` for quick iterations, longer runs for final
  numbers.
- Redirect stderr (`2>/dev/null`) to suppress the startup line when parsing
  JSON.
- Pipe multiple runs through `column -t` for aligned output.
