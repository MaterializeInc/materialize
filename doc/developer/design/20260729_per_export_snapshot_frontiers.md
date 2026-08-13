# Per-Export Frontiers for Source Snapshots

- Associated:
  [Subsource Snapshot Freshness: Options Summary](20260721_subsource_snapshot_freshness_research.md)

## The Problem

Adding a table to a running source (`CREATE TABLE ... FROM SOURCE`, or the legacy
`ALTER SOURCE ... ADD SUBSOURCE`) restarts the ingestion dataflow. On the new incarnation, the
added table has an empty resume upper and is snapshotted. Until that snapshot completes, no
subsource of the source makes visible progress. CDC events for already hydrated tables are not
processed, and their persist uppers do not advance. A snapshot of one large table can stall every
other subsource for hours.

If multiple tables are added together, snapshotting may complete for the smaller tables, but they
cannot make progress until the largest table is also done. Any error during this time restarts
snapshotting for all tables.

For the rewind-pattern connectors (PostgreSQL, MySQL, SQL Server) the stall has two causes, both
in the upstream reader operators:

1. The replication operator drains its rewind input to completion before processing any CDC
   events. Rewind requests are only sent after `COPY` (for OLTP) completes, so the CDC stream is
   not read while a snapshot runs.
2. All exports share a single data capability. While any rewind request is pending, the capability
   cannot be downgraded past the minimum offset, because the operator must still be able to emit
   retractions at the minimum timestamp.

Kafka stalls for a different reason. Multiple tables can be created from one Kafka source, for
example to accommodate schema changes in the topic. The reader resumes every partition from the
minimum resume upper across all exports, so adding a table makes the single consumer re-read the
topic from the new table's start offsets, and the shared output port means no export commits new
data until the re-read reaches the tip.

Everything downstream of the snapshot/replication operators is already per export: statistics
operator, reclock, decode and envelope, and persist sink.

## Success Criteria

- While a newly added table snapshots, the already hydrated exports of the same source continue to
  process CDC events and their persist uppers continue to advance.
- When multiple tables are added together, each commits its snapshot and begins advancing as soon
  as its own snapshot completes.
- Definiteness is preserved. The contents of every collection at each readable timestamp remain a
  deterministic function of durable shared facts. The output must not depend on any value known
  only to a single replica or dataflow incarnation, such as the snapshot LSN of the temporary
  slot.
- Exact multiplicities are preserved. No design element requires primary keys or upsert semantics
  from a connector that does not already have them.
- Memory on the replica during hydration is bounded by a configurable target independent of
  snapshot duration and of the number of distinct reclocked timestamps.
- The number of batches the sink appends is proportional to the number of batch descriptions, not
  to the number of distinct reclocked timestamps a stall accumulates.
- A restart during a snapshot remains correct. The snapshot restarts from the beginning, as today.

## Open Questions

- **Restart interleavings.**
  - Which restart-during-snapshot interleavings become newly possible once the snapshot and
  replication phases overlap, and what test coverage (platform checks, testdrive) demonstrates
  each is handled.
- **Eviction visibility.**
  - Whether the batch count produced by stash eviction needs a metric, so a stall that spreads
  evenly across many timestamps and degrades toward one batch per timestamp is visible rather than
  inferred from persist shard shape.
- **Multiple concurrent hydrations.**
  - The stash budget is per worker per export, so a replica's exposure is the budget times the
  number of exports snapshotting at once times the worker count. Whether the default holds up when
  several large tables hydrate together, or whether the budget should be a replica-wide pool.

## Out of Scope

The following are follow-on work. The design does not depend on them, and they can land
independently later.

- **Recovering accumulated work across restarts.**
  - A restart during hydration discards the hydrating export's snapshot and CDC data. On restart,
    both must be redone. Ensuring correctness on a restart would require substantial effort, and
    it isn't clear that it's needed.
- **Cleanup of leaked batches.**
  - Batches that were written to persist, but not linked in the shard are leaked. This is existing
    behavior, but was bound by snapshot size, and is now bound by snapshot + CDC during snapshot
    size.
- **Upsert storage increase due to concurrent streams.**
  - The current design for Kafka relies on a two consumer approach: backfill and steady state. In
    a mixed-export scenario (some exports are hydrating, some are steady state), the hydrating
    exports' frontier is pinned due to hydration. Records from both the snapshot and steady state
    will collect in the upsert's merge batcher, where before it was only snapshot records.
    Addressing this requires additional design around upsert.

## Solution Proposal

Give each export its own output port and capability on the snapshot/replication operators, and
hold back only the hydrating exports. Sources that use the rewind-pattern, the protocol is
unchanged. For Kafka, no reconciliation is needed, the backfill and steady feeds read disjoint
offset ranges. The change is a second consumer that backfills the hydrating exports.

The work is confined to each source. `SourceRender::render` already returns per-export
collections, reclock derives per-export output frontiers from per-export input frontiers without
changes, and remap bindings are minted from the probe channel independent of data capabilities.

Steps 1 and 4 apply to all sources. Steps 2 and 3 are the rewind-pattern changes, the Kafka
section below is their Kafka equivalent.

### 1. One output port per export

Replace the multiplexed `(output_index, data)` output and its `partition()` demux with one output
port per export on the snapshot and replication operators. Exports are known at render time, ports
are created in a loop, and each port gets its own capability set.

This step preserves existing behavior. All port capabilities downgraded in lockstep, every export
observes the same frontier as today.

For existing operators using a single output handle, `handle.give_fueled()` provided a means of
yielding at `MAX_OUTSTANDING_BYTES` to prevent the CDC stream from overwhelming downstream
operators. With the change to multiple output handles, operators must aggregate bytes emitted for
comparison against `MAX_OUTSTANDING_BYTES`.

There aren't any known issues related to number of input/ouptut ports in timely

### 2. Early snapshot bound

Modify the snapshot operator to emit rewind requests at the start of the snapshot instead of at
completion. For Postgres, the snapshot LSN is the temporary slot's consistent point and is known
as soon as the slot is created, before any data is copied. The `resume_lsn <= snapshot_lsn + 1`
validation moves to this earlier point.

The replication operator no longer drains the rewind input before opening the replication stream.
It starts streaming immediately, knowing from the early rewind requests which exports are
hydrating.

### 3. Per-export capability policy

**Snapshot operator:** ports of exports the operator will not emit snapshot data on are closed
immediately. That covers exports whose resume upper is past the minimum and exports that carry no
snapshot data at all, such as a source's primary relation. A hydrating export's port holds at the
minimum until its snapshot (e.g. `COPY`) completes, then closes.

**Replication operator:** each port of hydrated exports downgrades with the data upper. A
hydrating export's port holds at the minimum until the data upper passes its snapshot offset,
because events at or below the snapshot offset must still be emitted negated at the minimum for
that export. Once the upper passes the snapshot offset, the port downgrades with the data upper
like the rest. The emission of rewind negations remains unchanged.

Sources still acknowledge at the minimum committed upper across exports. For PG, a hydrating
export continues to pin WAL retention, but no longer pins any other export's frontier.

### 4. Bounded sink accumulation

A batch's bounds come from a batch description, which `mint_batch_descriptions` emits when the
collection's frontier advances. While an export snapshots its frontier is pinned, so no
description is minted for the duration. The sink still receives snapshot rows at the pinned time
and CDC events at later times, and must hold them until it learns which description covers them.

Sources have the time domains `F` and `T`, `FromTime` and `MzTime`, respectively. The snapshot
operator emits data at `F:min`, which is reclocked to `T:c`, where `T:c` is the current time. The
snapshot operator pins its capability to `F:min`, and by extension, `T:c`. The replication
operator emits data at from-times `f >= F:min`, that are mapped to MZ times `t >= T:c`. The
replication operator downgrades caps as today.

Because snapshot has pinned its capability at `F:min`, the downgrades of the replication operator
do not move the frontier forward, so all downstream operators see events reclocked to the correct
`T` times, but the frontier does not advance.

Once the snapshot is done it drops its capability, timely propagates some time `T:n`, and the
minter emits a single description `[T:c, T:n)` covering everything that accumulated during the
stall.

Stage arriving updates in the sink as raw rows keyed by timestamp instead of opening a
`BatchBuilder` per timestamp. A description is only acted on once the frontier has reached its
upper, so every update it covers has already arrived and one builder can take all of them. This
keeps the batch count proportional to the number of descriptions rather than to the number of
distinct reclocked timestamps the stall accumulated. Building on arrival cannot do this. The
grouping would have to be chosen before the bounds are known, and a `BatchBuilder` cannot be split
once a description boundary lands inside it.

`storage_persist_sink_max_raw_stash_bytes` bounds the stash, per worker per export. Over budget the
sink consolidates first. At the pinned timestamp the snapshot's rows and the rewind retractions
that supersede them are both staged and cancel exactly, so consolidation often reclaims the excess
without writing anything. If it does not, the heaviest timestamps are written out into
single-timestamp builders. A single timestamp is safe to write before its description exists,
because a description covers a timestamp entirely or not at all, so such a builder cannot straddle
a boundary.

Eviction costs one batch per timestamp evicted. Taking the heaviest first does well when volume
concentrates in a few timestamps and poorly when it spreads evenly across many, so a stall staging
far more than the budget with an even spread converges toward one batch per timestamp. That is the
behavior before this change rather than a regression.

Batches now span multiple timestamps, which does not change the recovery path. When a concurrent
writer raises the shard upper into the middle of a description, the sink advances the description's
lower and re-appends. Persist registers a batch under the narrowed description and filters the
updates outside those bounds on read, so a batch holding data on both sides of the new lower stays
usable: the updates the concurrent writer already committed do not come back, and the ones the sink
still owes are preserved. Each batch carries the largest timestamp it holds, which is enough to
delete the batches lying entirely below the new lower rather than registering parts that would be
truncated away in full.

#### Statistics semantics

`updates_staged` counts on arrival rather than when an update reaches a builder, so a pinned
frontier does not make the sink look idle while it is ingesting hard. `updates_committed` counts at
append, from batch metrics that only cover updates which survived consolidation.

The two therefore relate as `updates_staged >= updates_committed`, and the gap is whatever the
stash consolidated away. Before this change the sink never consolidated, so the two matched exactly
for every workload. The gap is information rather than drift, it reports the work the sink avoided
writing. Back to back upstream transactions that reclock to the same timestamp and touch the same
row are the ordinary way to produce one.

#### Handling for large xacts

If an upstream transaction contains a large number of rows they all land at a single timestamp in
the stash. That is what eviction takes first, so it is written out on its own rather than held in
memory. Memory stays bounded, at the cost of one batch for that transaction.

### Kafka: backfill consumer with offset handoff

Kafka has no rewind machinery. The "snapshot" of a new export is a re-read of the topic from the
export's start offsets, and "CDC" events are the data after the snapshot (determined using kafka
offsets). If there are 2 exports for the same topic, the stall of the hydrated export is a result
of the shared Kafka consumer having to read from the export's start offsets.

The kafka source may have up to 2 consumers: a backfill consumer for all exports that need to be
hydrated, and a consumer for steady state. If no hydration is necessary, or all exports need
hydration, only a single consumer is created. This limits the two consumer case to the situation
that motivates it: adding a new export where one already exists.

For the mixed export case, a frontier `B` is determined on startup. The backfill consumer will
capture rows below `B`, and the steady state consumer captures rows at or above `B`. The two
consumers emit concurrently for hydrating exports. As today with transition from hydration to
steady state, the handoff at frontier `B` requires no dedup or negation. This is possible because
the set of records for the snapshot and the set of records for steady state are disjoint. The
logical choice for `B` is the minimum `resume_upper` for all non-hydrating exports in the mixed
export case. For the case where all exports are hydrating, `B` keeps its existing definition,
which is the maximum frontier (maximum offsets of each partition).

The existing Kafka behavior for reporting snapshot progress is to capture the maximum offsets for
each partition in the topic, sum them, and treat them as `snapshot_records_known`. In the mixed
export case, snapshot and steady state records are being processed concurrently. To accurately
record statistics, metrics definitions are updated.

1. `snapshot_records_known` would be `sum(B - start_offsets)`
2. `snapshot_records_staged` counts backfill progress only
3. `updates_staged` counts steady state records (doesn't include backfill)

This fixes an issue in the existing implementation, which ignores `start_offsets` in the
calculation of `snapshot_records_known`. This doesn't change the issue that `B - start_offsets`
overcounts on compacted topics.

For upsert, correctness is maintained as snapshot and steady state records land in the merge
batcher, and are only emitted on frontier progression. Because hydrating exports will have their
frontier pinned, updates accumulate in the upsert merge batcher, increasing memory utilization.
The increase is the concurrent steady state updates. The batcher pages cold data out of resident
memory, but the staged volume itself grows with the hydration duration. Additionally, the merge
batcher drain is sequential per timestamp. For a long hydration period, this drain would cover
thousands of timestamps, each making a round-trip through persist.

The previous upsert implementation (rocksdb) used a partial drain during snapshots. The staged
updates merge into the state store before the frontier advances. The state still exists, but it
moves out of operator memory into rocksdb, and updates to the same key collapse to a single
value, so the retained state is proportional to unique keys rather than to update count. That
collapse is correct because a snapshot is a single timestamp. Every update resolves to its final
value at that time, and no intermediate output is emitted, so nothing is lost by merging early.
The concurrent steady state updates span many timestamps. Correct output needs a retraction and
insertion pair at each time a key changes, so entries for the same key at different times cannot
collapse, and the staged volume grows with update count no matter where it is stored. Partial
drain does not extend to this case.

Upsert needs rework to cover both hydration cases: the large single-timestamp snapshot, and the
multi-timestamp steady state data staged behind a pinned frontier.

The drain latency can be fixed within the current architecture. When persist reaches `p` and the
batcher holds times `[p, input_upper)`, process the whole run in one iteration by walking the
staged times in order against an in-memory overlay seeded from the trace at `p`. This fixes the
sequential round-trips but not the staged volume.

Processing staged times ahead of persist, against an overlay of committed state plus the
operator's own uncommitted output, was considered and does not work. Data is routed through
persist to handle multiple replicas. Replicas race to append, and a replica's uncommitted output
is not the truth. Persist must remain the source of truth for prior values, which is what the
frontier-gated eligibility enforces. Bounding the staged volume during hydration therefore
remains open, and the rework needs a design of its own. This design does not depend on it.

### Restart semantics

A restart during hydration discards the accumulated updates, they were never linked into the
shard, and the snapshot restarts from the beginning, for the rewind connectors with a new
temporary slot and a new snapshot LSN, for Kafka with a new backfill from the export's start
offsets. This matches today's semantics. The serialization that today makes some restart
interleavings impossible by construction is removed, so restart-during-snapshot paths need
explicit re-verification (see Open questions).

### Rollout

PostgreSQL first, then MySQL and SQL Server, which use the same rewind structure adapted to their
offset types. MySQL keys `RewindRequest` by a GTID snapshot upper, SQL Server already tracks a
per-export `initial_lsn` and `snapshot_lsn` pair. In both, the snapshot upper is available when
the snapshot transaction is established, so the same three operator changes apply. Kafka follows,
since its backfill consumer is new machinery rather than an adaptation of the rewind changes.

It's not clear if Kafka is worth doing, at least until upsert memory utilization during hydration
in the mixed export case is addressed.

The behavior change (steps 2 and 3) is gated by the
`storage_source_snapshot_concurrent_replication` feature flag, default off in production and
default on in CI so the new path is exercised by the test suites before it is enabled.

In general, a connector supports independent export frontiers when it can (a) emit each export on
its own output port with its own capabilities, (b) determine the snapshot consistency bound when
the snapshot starts rather than when it completes, (c) reconcile snapshot and stream either by
compensating negated emission at the minimum time or through an envelope that derives retractions
from state, and (d) derive per-export progress from its own read process.

## Considered but Rejected

The research document records the full option space. Summary of the rejected options and why:

- **DBLog-style watermark snapshots.**
  - Interleaving chunked snapshot reads with the stream, deduplicating by primary key between
  watermarks, produces a per-key upsert stream with no consistent point-in-time state. Rejected
  because storage collections require exact multiplicities, the nondeterministic interleaving
  violates definiteness, and all sources would now require upsert.
- **Persist extensions as the primary mechanism.**
  - Gap appends or placeholder batches would make concurrent CDC durable immediately and remove the
  sink accumulation problem entirely, and a staged batch registry would make the coalesced builders
  crash-safe. Rejected for the initial delivery because they change persist shard state, compaction,
  and read paths.
- **Out-of-band snapshot committed directly to persist.**
  - Run the snapshot as a one-shot job and `compare_and_append` the result, then restart the main
  dataflow to pick up the export, which is now past the snapshot, so no other exports are affected.
  Rejected because this requires new infrastructure that this design does not: a one-shot job
  outside the ingestion dataflow, a durable handoff record, verification that the slot has not
  compacted past the recorded LSN, and per-export start points threaded into the dataflow.
- **Shadow pipeline with catch-up and handoff.**
  - A second temporary pipeline with its own replication slot snapshots the new table, follows CDC
  to an agreed LSN, and hands off. Rejected as this does not improve slot's WAL retention, adds
  connection cost on the upstream database, and requires an exactly-once handoff protocol.

The following were considered for step 4 specifically, and all share a root cause. Each tries to
choose a batch's grouping before its bounds are known, which is the thing that cannot be done
safely.

- **Trailing-lag stash.**
  - Age rows out of the stash into a coalesced builder once they fall a fixed lag `L` behind the
  latest data time, and rotate that builder when the frontier advances. Rejected because `L` must
  exceed the frontier's propagation delay, which is not a quantity the sink can bound, and
  exceeding it is only detected at rotation, where the recovery is a dataflow restart.
- **A coalescing horizon published by the minter.**
  - Have `mint_batch_descriptions` broadcast a promise that no future description will end below
  some time, so write operators can group updates below it before their description exists.
  Rejected because the promise is derived from the minter's frontier, which is pinned for exactly
  the duration of a snapshot, so it cannot advance during the stall it exists to cover.
- **Source read progress as the coalescing horizon.**
  - Feed the source's reclocked read progress into the minter so the promise can advance while the
  collection's frontier is pinned. Rejected because it adds a progress marker to every source's
  dataflow to serve one sink concern, and each new source pays that cost again.
- **Splitting the pinned traffic onto its own output.**
  - Give the traffic at the minimum offset, the snapshot rows and the rewind retractions, a
  separate output so the ongoing replication stream keeps an unpinned frontier. Rejected because
  the split is not static. The replication operator's schema-validation errors are emitted at
  whatever the port's capability currently holds, which is the minimum only while a rewind is
  pending, so routing them has no fixed answer. Envelope processing in `render_source_stream` is
  also stateful for upsert and cannot carry a split through in general.
- **Pager-backed spill for the stash.**
  - Evict stashed rows into `mz_ore::pager` chunks rather than into persist batches, keeping the
  batch count at one per description regardless of budget. Rejected for now because the pager's
  default backend is swap, which is ordinary heap plus a reclaim hint and so does not change the
  bound. Only the file backend does, and it depends on a scratch directory and on a process-global
  setting that compute owns.
