# Persist

Persist is an implementation detail of STORAGE. Its "public" API is used only
by STORAGE and code outside of STORAGE should be talking to STORAGE, not
persist. However, having this strong API boundary between the two allows for the
two teams to execute independently.

Persist's primary abstraction is a "shard", which is a durable and definite
[Time-Varying Collection (TVC)]. Persist requires that the collection's "data"
is key-value structured (a `()` unit value is fine) but otherwise allows the
key, value, time, and diff to be abstract in terms of the [Codec] and [Codec64]
encode/decode traits. As a result, persist is independent of Materialize's
internal data formats: `Row` etc.

[Time-Varying Collection (TVC)]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/platform/formalism.md#time-varying-collections
[Codec]: mz_persist_types::Codec
[Codec64]: mz_persist_types::Codec64

Along with talking to the outside world, STORAGE pilots persist shards around to
present a "STORAGE collection", which is a durable TVC that handles reshardings
and version upgrades. A persist shard is exactly a [storage shard] and they can be used
interchangeably.

[storage shard]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/platform/architecture-storage.md#shards

More details available in the [persist design doc].

[persist design doc]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20220330_persist.md

## FAQ: What is persist's throughput?

In general, with proper usage and hardware (and once we finish tuning), persist
should be able to saturate 75% or more of the available network bandwidth on
both writes and reads.

Experimentally, the `s3_pg` configuration has sustained 64 MiB/s of goodput for
10+ hours in an open loop benchmark. This is nowhere near our max, but should
easily be sufficient for M1. TODO: Add numbers under contention.

```sh
cargo run -p mz-persist-client --bin persist_open_loop_benchmark --blob_uri=... --consensus_uri=...
```


## FAQ: What is persist's latency?

Materialize is not an OLTP database, so our initial tunings are for throughput
over latency. There are some tricks we can play in the future to get these
latencies down, but here's a quick idea of where we're starting.

The vertical axis:
- `mem_mem` uses im-memory implementations of "external durability". These exist
  for testing but here they're nice because they show the overhead of persist
  itself.
- `file_pg` uses files for blob and Postgres for consensus. This is what you
  might expect in local development or in CI. (These numbers, like the rest, are
  from a persist-benchmarking ./bin/scratch box. Maybe this one should be from a
  laptop?)
- `s3_pg` uses s3 for blob and AWS Postgres Aurora for consensus. This is what
  you might expect in production.

The horizontal axis:
- `write` is an un-contended small write (append/compare_and_append).
- `wtl` (write_to_listen) is the total latency between the beginning of a
  small write and it being emitted by a listener. Think of this as persist's
  contribution to the latency between `INSERT`-ing a row into Materialize and
  getting it back out with `SELECT`.
- The `(est)` variant is whatever Criterion uses to select its "best estimate"
  and `(p95)` is the higher end of Criterion's confidence interval (not actually
  a p95 but sorta like one).
- TODO: Real p50/p95/p99/max.

| | write (est) | write (p95) | wtl (est) | wtl (p95) |
| --- | --- | --- | --- | --- |
| mem_mem | < 1ms | <1ms | 5.1ms | 5.2ms |
| file_pg | 5.5ms | 5.6ms | 6.4ms | 6.5ms |
| s3_pg | 45ms | 47ms | 79ms | 82ms |

These numbers are from our micro-benchmarks.

```sh
cargo bench -p mz-persist-client --bench=benches
```

Larger writes are expected to take the above latency floor plus however long it
takes to write the extra data to e.g s3. TODO: Get real numbers for larger
batches.


## Memory Usage

_Note: This is provisional and based on mental modeling, it is yet to be
empirically tested._

A persist writer uses at most `B * (2N+1)` memory per [BatchBuilder] (available
for direct use and also used internally in the `batch`, `compare_and_append`,
and `append` sugar methods). This is true even when writing data that is far
bigger than this cap.

- `B` is [blob_target_size]
- `N` is [batch_builder_max_outstanding_parts]
- `2N` because there is a moment that we have both a ColumnarRecords and its
  encoded representation in memory.
- `+1` because we're building the next part while we have `N` of them
  outstanding.

[BatchBuilder]: crate::batch::BatchBuilder
[blob_target_size]: crate::PersistConfig::blob_target_size
[batch_builder_max_outstanding_parts]: crate::PersistConfig::batch_builder_max_outstanding_parts

A persist reader uses as most `3B` memory per [Listen] and per [SnapshotIter].

- `B` is [blob_target_size]
- We have one part fetched that is being iterated
- In the future, we'll pipeline the fetch of a second part. Like writing there
  is a moment where both a ColumnarRecords and its encoded representation are in
  memory.

[Listen]: crate::read::Listen
[SnapshotIter]: crate::read::SnapshotIter

Both of these might have _small_ additive and multiplicative constants. This is
true even when reading data that is far bigger than this cap.


## OpenTelemetry Tracing Spans

Persist offers introspection into performance and behavior through integration
with the [tracing] crate.

[tracing]: https://docs.rs/tracing

Materialize defaults to logs (tracing events) at "info" and above and
opentelemetry (tracing spans) at "debug" and above. This is because our stderr
log formatter includes any spans that are active at the time of the log event
AND that match the log (not opentelemetry) filter. Example of what this looks
like:

```text
2022-06-02T21:18:48.220658Z  INFO my_span{my_field=foo}: my_crate::my_module: Hello, world!
```

So in practice, with the default flag settings, spans at:

- _info_: Exported to opentelemetry AND included in any event logs that happen
  while the span is active as described above. Nothing in persist meets this
  bar.
- _debug_: Exported to opentelemetry (but not included in logs).
- _trace_: Completely disabled.

There are many policies we could adopt for what is instrumented and at what
level. At the same time, it's early enough that there are lots of unknowns
around what will be most useful for debugging real problems and what knobs are
wanted to opt into additional detail. As a result, we adopt a common idiom of
spans at the API boundary between persist and the rest of mz (as well as
persist's API boundary with external systems such as s3 and aurora, which we
wouldn't do if they themselves offered tracing integration). In detail:

- All persist spans have a `shard` field and no other fields.
- Every method in the persist public API is traced at debug level. However, this
  excludes "sugar" methods (e.g. append and snapshot) that are implemented
  entirely in terms of other public persist API methods, which are traced at
  trace level.
- All writes to external systems are traced at debug level. Reads from S3 are
  also traced at debug level. Reads from Aurora are traced at trace level (too
  spammy).
- Additional debugging information is traced at trace level.

We'll tune this policy over time as we gain experience debugging persist in
production.
