# Freshness Histograms

- Associated: (TBD — link issues/epics once created)

## The Problem

Materialize records wallclock lag — the difference between the current time
and a collection's write frontier — to characterize data freshness. Today,
this measurement has several limitations:

1. **Hardcoded 1-second rounding.** `WallclockLagFn` rounds all measurements
   up to the nearest second. This made sense when all sources ticked at 1s,
   but with per-object tick rates (potentially 100ms or faster), the
   measurement system cannot distinguish meaningful sub-second freshness
   differences.

2. **No distributional information.** The system records individual lag
   samples (one per object per second) and a sliding 60-second min/max.
   There is no way to answer "what is the p99 or p99.999 freshness over
   the last hour/day/week?"

3. **No latency decomposition.** The single `now() - frontier` number
   conflates ingestion delay, dataflow processing delay, and controller
   reporting delay. When freshness is worse than expected, operators have no
   way to determine which component is responsible.

4. **Storage scaling concerns.** Per-second raw samples produce ~86K
   rows/day/object. With faster tick rates, this grows proportionally (864K
   rows/day/object at 100ms ticks), making the current approach untenable.

## Success Criteria

- **Sub-second precision.** The system records freshness at the precision of
  the object's effective tick rate, without artificial rounding.

- **Percentile queries.** Users and operators can query freshness percentiles
  (p50, p99, p99.999) per object over configurable time windows, via SQL.

- **p99.999 over a week.** At 100ms tick rates, the system collects enough
  samples (~6M/week) that 5-nines percentiles are statistically meaningful
  (~60 tail samples).

- **Storage efficiency.** Histogram storage is comparable to or less than the
  current per-second raw sample approach. Sparse histograms (only non-zero
  buckets stored) keep steady-state storage low (~3-5 rows per object per
  window) while allowing pathological cases to use more.

- **Latency decomposition (model).** The freshness model formally decomposes
  end-to-end freshness into input freshness, processing delay, and reporting
  delay, even if not all components are surfaced in SQL from day one.

- **No regression.** Existing monitoring that depends on wallclock lag
  Prometheus metrics continues to work during the transition.

## Out of Scope

- **Per-dataflow decomposition in SQL.** The model supports decomposing
  freshness along the dataflow graph (diffing upstream vs. downstream output
  freshness). Surfacing this in SQL tables is deferred to future work; the
  histogram infrastructure will support it without schema changes.

- **Replica-side measurement.** Measuring processing delay on the replica
  (where it can be observed most accurately, without clock skew) requires
  protocol changes. This is a natural follow-up but not part of the initial
  implementation.

- **Time-series encoding.** Storing raw frontier transition events
  (`(object_id, frontier_ts, wallclock_ts)`) would be ideal but requires
  time-series storage primitives that Materialize does not have today.

- **Multi-resolution rollups.** Keeping fine-grained histograms for recent
  data and rolling up to coarser windows for long-term retention adds
  complexity. Deferred unless storage budget demands it.

## Solution Proposal

### Freshness Model

For a given object O at wallclock time W:

```
freshness(O, W) = W - frontier(O, W)
```

where `frontier(O, W)` is the largest timestamp T such that all data up to T
is reflected in O's output as observable by a user at time W.

Freshness decomposes along the dataflow graph. For an object O with input
objects I_1, ..., I_n:

```
freshness(O, W) = input_freshness(O, W) + processing_delay(O, W) + reporting_delay(O, W)
```

where:

- **input_freshness(O, W) = max(freshness(I_1, W), ..., freshness(I_n, W))**
  — freshness of the slowest input.
- **processing_delay(O, W)** — time between the input frontier advancing and
  O's output frontier advancing, measured on the replica.
- **reporting_delay(O, W)** — time between the output frontier advancing on
  the replica and the controller observing it.

For a source with no dataflow inputs, `input_freshness` is the ingestion
delay (wallclock time of data arrival minus the assigned timestamp), and
there is no processing delay.

This model is the conceptual foundation. The initial implementation measures
`freshness(O, W)` end-to-end on the controller. The decomposition into
components is enabled by the model and histogram infrastructure but surfaced
incrementally.

### Histogram Bucket Scheme

Buckets use an exponential scheme with subdivision, similar to floating-point
representation:

- **N bits "whole"**: determines the power-of-2 range (e.g., 1ms, 2ms, 4ms,
  ..., 2^N ms).
- **M bits "subdivision"**: divides each power-of-2 range into 2^M equal
  steps.

For example, with N=20, M=2 (4 subdivisions per range):
- In the 100ms-200ms range: buckets at 100ms, 125ms, 150ms, 175ms.
- In the 1s-2s range: buckets at 1.0s, 1.25s, 1.5s, 1.75s.

Total possible buckets = N * 2^M, but only non-zero buckets are stored.
Relative resolution is uniform (~1/2^M per bucket regardless of scale).

The exact values of N and M are configurable via dyncfg. They trade off
bucket resolution against worst-case storage (pathological distributions that
spread across many buckets).

### Controller-Side Histogram Maintenance

On each frontier update the controller receives for a collection:

1. Compute raw `freshness = now() - frontier` (no rounding).
2. Map to a bucket index using the (N, M) exponential scheme.
3. Increment the count for that bucket in the current window's in-memory
   histogram for that (object, replica) pair.
4. On window close (e.g., every 60 seconds, configurable via dyncfg), flush
   all non-zero `(bucket_i, count)` pairs to the introspection table and
   reset the in-memory state.

The `WallclockLagFn` 1-second rounding is removed.

### Storage Schema

The SQL-facing histogram table stores:

```sql
(object_id, replica_id, window_start, bucket_i, count)
```

where `count` is the number of freshness observations in the half-open
interval `(bucket_{i-1}, bucket_i]` during the window starting at
`window_start`.

Percentile queries are computed in SQL by scanning buckets, accumulating
counts, and interpolating within the bucket that contains the target
percentile.

**Storage analysis (steady state, ~3 non-zero buckets per window):**

| Window size | Rows/day/object | vs. current 86.4K rows/day |
|-------------|----------------:|---------------------------:|
| 1 min       | 4,320           | 0.05x (20x cheaper)        |
| 10 sec      | 25,920          | 0.3x                       |

Pathological cases (lag spread across many buckets) store more rows, but this
is self-limiting and correlates with diagnostic value.

### Controller Reporting Delay

The delay between a frontier advancing on the replica and the controller
observing it is tracked as a **Prometheus histogram only** (not SQL-facing).
This establishes measurement confidence and characterizes the overhead of
controller-side observation.

Clock skew between replica and controller means this metric is approximate,
but sufficient for internal diagnostics.

### Migration

The new histogram table complements or replaces `mz_wallclock_lag_history`.
The existing Prometheus metrics (`mz_dataflow_wallclock_lag_seconds` gauge
with sliding min/max, sum, count) are retained during the transition to avoid
breaking existing monitoring.

### Future Extensions

The design supports natural follow-up work without schema changes:

- **Per-dataflow decomposition**: Each dataflow output already has a distinct
  `object_id`. Recording histograms for all outputs (not just user-facing
  collections) enables decomposition by diffing upstream and downstream
  freshness distributions.

- **Replica-annotated frontier updates**: Adding a `replica_wallclock`
  timestamp to frontier responses enables the controller to histogram
  processing-relevant freshness (`replica_wallclock - frontier`) separately,
  cleanly decomposing processing delay from reporting delay.

- **Multi-resolution rollups**: Merging histograms across windows is trivial
  (sum counts per bucket). A background process could roll up fine-grained
  windows into coarser ones for long-term retention.

## Minimal Viable Prototype

The prototype should demonstrate:

1. Exponential histogram bucket mapping (N, M configurable).
2. Controller maintaining in-memory histograms per (object, replica).
3. Flushing sparse histograms to an introspection table.
4. A SQL query that computes approximate percentiles from the histogram.

This can be validated on a local Materialize instance by comparing histogram-
derived percentiles against raw lag samples collected in parallel.

## Alternatives

### A: Keep per-second raw samples, increase precision

Remove the 1s rounding but continue storing one raw lag value per second.
Rejected because storage scales linearly with tick rate (864K rows/day/object
at 100ms), and percentile computation over raw samples is expensive at query
time.

### B: Replica-side histograms shipped to controller

Have replicas maintain histograms locally and periodically ship snapshots to
the controller for merging. This gives the most accurate processing delay
measurement (no clock skew) but adds significant complexity: histogram merge
logic, new protocol messages, and stateful metric tracking on replicas.
Deferred as a follow-up to the controller-side approach.

### C: Store frontier transition events

Record `(object_id, frontier_ts, wallclock_ts)` for every frontier advance
and derive histograms as SQL views. This preserves maximum information but is
textbook time-series data, and Materialize lacks time-series encoding
primitives. Storage cost is comparable to raw samples.

### D: Sketch-based approaches (DDSketch, t-digest)

Use mergeable sketches instead of fixed-bucket histograms. These offer better
space/accuracy trade-offs for extreme percentiles but add implementation
complexity and are harder to expose through SQL's `(key, bucket_i, count)`
pattern. The exponential histogram with subdivision provides comparable
relative accuracy with a simpler, SQL-native representation.

## Open questions

- **What values of N and M?** These determine bucket resolution and worst-
  case storage. Need to be sized based on the range of expected freshness
  values (sub-100ms to hours) and acceptable relative error at tail
  percentiles. Should be dyncfg-tunable.

- **Window size?** 1-minute windows are ~20x cheaper than today's storage in
  steady state. Shorter windows give better temporal resolution for
  diagnosis. The right default depends on retention requirements and query
  patterns.

- **Replace or complement `mz_wallclock_lag_history`?** The histogram table
  is strictly more informative, but changing the existing table is a
  user-facing breaking change. Need to decide on migration strategy.

- **Histogram bucket boundaries: inclusive/exclusive?** The half-open
  interval convention `(bucket_{i-1}, bucket_i]` needs to be specified
  precisely, especially for the lowest bucket (observations of zero lag).

- **Reporting delay Prometheus histogram bucket scheme.** Should it use the
  same (N, M) exponential scheme for consistency, or a simpler set of fixed
  buckets since it's internal-only?
