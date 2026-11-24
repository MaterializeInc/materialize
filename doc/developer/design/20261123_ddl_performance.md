# DDL performance: incremental durable catalog state

## Outcome and boundaries

Small catalog mutations should not rebuild unchanged durable catalog state.
The implementation removes two catalog-size-dependent costs without changing
durability, visibility, fencing, transaction isolation, or the durable format.
It does not introduce parallel durable writers or acknowledge DDL before its
required publication and controller work completes.

The durable handle retains `DecodedCatalogState`, typed Rust key/value maps
distinct from the adapter's planning objects and lookup indexes. Transactions
share immutable map bases and keep private pending writes. Transaction entry
still synchronizes with durable state and validates fencing before sharing the
base. Incoming updates refresh the decoded representation at the durable update
boundary. Commit releases transaction references before another refresh.
Decoding errors invalidate the partial base so it can be rebuilt from the trace.
Explicit snapshots and captured dry-run snapshots retain their existing behavior.

The raw durable trace uses an ordered map of consolidated keys and signed
multiplicities, a shared timestamp, and a buffer of incoming updates. Consolidation
merges that buffer rather than sorting and retimestamping every unchanged row.
Raw JSON identities remain distinct even when they decode to the same protobuf
value. Canceled updates still affect the retained timestamp as required by the
vector algorithm. Empty progress and filtered audit/fence updates do not advance
that timestamp. Cold initialization and explicit materialization still visit
the whole snapshot.

## Measurement

The focused workload measures CREATE/DROP tables, views, and indexes against
5k, 10k, and 30k mixed catalog objects with one and eight autocommit clients.
Padding includes live table/index infrastructure. Measurements cover SQL
acknowledgement, not hydration. Middle repetitions carry CPU profiles and are
excluded from the client comparison.

| Change | Observed 30k result | Evidence |
|---|---|---|
| Shared decoded state | Throughput +22-44%, local snapshot/base refresh plus initialization about 31 ms to 0.02 ms, excluding synchronization | [Baseline 237](https://buildkite.com/materialize/spec-sheet/builds/237), [candidate 238](https://buildkite.com/materialize/spec-sheet/builds/238) |
| Incremental trace consolidation, relative to shared decoded state | Throughput +9-19%, CPU/statement -13-20%, catalog transact about 32 ms to 20-21 ms | [Paired rerun 240](https://buildkite.com/materialize/spec-sheet/builds/240) |

These are separate comparisons, not additive or compounded gains. Both use
ARM64 environmentd with 2 CPU and 16 GiB limits. The first comparison has an
unresolved effective-override count difference and sparse telemetry. Its 5k
result is mixed, including higher sampled CPU. It is not a configuration-pinned
causal estimate.

For consolidation, [build 239](https://buildkite.com/materialize/spec-sheet/builds/239)
supplies the valid 5k comparison. Its later points were invalidated when observer
authentication expired, removing metric-gathering load. Build 240 repeats both
runtimes at 10k and 30k on a shared account, node, and driver agent. Configuration
fingerprints match within and across those builds. These are checkpoints, not
a freeze of live flags. The rerun starts at 10k without earlier 5k churn.

Consolidation improves throughput across all 18 object/client/size cases, but
does not improve every measurement. CPU per completion increases 2% for 10k
tables with eight clients, and p99 latency is mixed. The 30k candidate
view/eight-client case has no eligible timer/backend pair. Its client, CPU,
and sampled memory measurements remain usable.

The decoded-state comparison uses one ascending sweep per binary. The trace
comparison has one paired observation per size, split across two builds. Neither
comparison has confidence intervals.
CPU windows include background work, observer work, and bounded batches.
Timers nest and have distinct denominators, so their means are not additive
components of client latency. Logical persist and oracle counters include
background work and are not exact CRDB SQL/KV requests per DDL. No consistent
backend-call reduction is established. Sampled RSS stays well below the 16 GiB
limit, but the measurements do not isolate retained data-structure footprints.

## Verification contracts

Coverage includes commit/abort isolation, replacement and deletion, captured
snapshots, fallible decoding and recovery, bounded pending updates, fencing,
read-only handles, savepoints, and reopening. Incremental trace tests compare
against full-vector consolidation, including cancellation timestamps and signed
multiplicities. Repair fixtures retain raw rows that decode equally.

## Remaining work

The next small changes should avoid scans whose results cannot affect the DDL:

- Count resources only when quota validation sees a positive net delta. Preserve
  signed netting, source weights, and error ordering.
- Skip uniqueness traversal when `TableTransaction::verify_keys` receives no
  live keys. Preserve optional full soft-assert verification.

Other `for_values` callers include OID allocation, insertion, comment deletion,
and storage metadata enumeration. Storage preparation also reconciles shard
finalization and cannot simply be omitted.

The larger architectural direction is concurrent preparation, ordered validation
and durable commit, ordered publication and controller updates, then asynchronous
completion before acknowledgement. The concurrent-DDL command framework already
preserves intent and replays against the current catalog. It is not a generic
catalog read-set validation engine.

Extending this framework to ordinary table/view/index DDL must preserve table
registration/write/forget ordering, generation fencing, catalog revision
publication, and builtin visibility. Do not remove the DDL lock from paths that
rely on it to protect prepared plans, add parallel durable writers, or acknowledge
success before required completion work finishes.
