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
- A restart during a snapshot remains correct. The snapshot restarts from the beginning, as today.

## Open Questions

- **Restart interleavings.**
  - Which restart-during-snapshot interleavings become newly possible once the snapshot and
  replication phases overlap, and what test coverage (platform checks, testdrive) demonstrates
  each is handled.
- **Choosing Lag.**
  - How the lag `L` is chosen and validated (a fixed configurable value, or adaptive to observed
  description propagation delay), and whether the restart fallback when the lag is exceeded needs a
  metric and alert since it silently costs a rehydration.
- **Multiple concurrent hydrations.**
  - Whether the sink change needs per-export tuning when several large tables hydrate at once on one
  replica.

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

The main concept here is to create a builder, the coalesced builder, that snapshot data and
replication data will route to. `BatchBuilder` already spills to blob storage, which bounds
memory usage. The challenge with this approach is that data can outrun the frontier change that
defines the upper of the batch. The design needs to account for learning the upper of the batch
via a frontier change, where some data may have timestamps beyond that upper and should be written
to the next batch.

Sources have the time domains `F` and `T`, `FromTime` and `MzTime`, respectively. The snapshot
operator emits data at `F:min`, which is reclocked to `T:c`, where `T:c` is the current time. The
snapshot operator pins its capability to `F:min`, and by extension, `T:c`. The replication
operator emits data at from-times `f >= F:min`, that are mapped to MZ times `t >= T:c`. The
replication operator downgrades caps as today.

Because snapshot has pinned its capability at `F:min`, the downgrades of the replication operator
do not move the frontier forward, so all downstream operators see events reclocked to the correct
`T` times, but the frontier does not advance, which keeps the builder open.

Once the snapshot is done, it will drop its capability, allowing timely to propagate some time,
`T:n`, and that will establish the end of the batch that both snapshot events and CDC events are
being written to.

Timely does not guarantee the ordering of data and progress messages. So it is expected that
data, reclocked to some `t >= T:n`, have made their way to the persist sink. They cannot be
written to the coalesced batch, which will only include data for `[T:min, T:n)`. The batch's
lower bound here comes from the shard upper, which is `T:min` for a shard that's not been written
to. The persist sink must error rather than write a row into a batch whose bounds do not cover it.

To prevent those rows from finding their way into that batch, we modify the persist sink to stash
new rows before writing to the batch. Data lands in the stash and lives there for a time
determined by the lag `L`, which is some number of timestamps. As data arrives, data from the
stash is aged out according to the latest data time, into the coalesced builder. When the
frontier does advance, the coalesced builder is adopted as `[T:min, T:n)`, and remaining stashed
rows route to it or to a newer batch. The lag, `L`, must exceed the propagation delay of the
frontier, otherwise data from the stash will have aged out into the coalesced builder. The
persist sink will detect data beyond the bounds of the builder when the builder is rotated and
error, which discards the unlinked batches.

Resident memory is bounded for both the coalesced builder and the stash. Each coalesced builder
holds at most `blob_target_size` plus one part upload in flight, and open builders are limited to
the number of in-flight descriptions plus one. The stash contains only data within the trailing
window `L`, so its size is bounded by `L` times the ingest rate, rather than by snapshot duration.

In steady state, updates enter their description's builder on arrival or drain into it when the
description finishes (i.e. the open builder stays empty). The open builder fills only when the
frontier stalls for longer than `L`, which is the hydration case. The API and crash-leak behavior
of persist remains unchanged from today.

#### Handling for large xacts

If an upstream transaction contains a large number of rows, they will all land in the stash and
put pressure on memory. The prototype stores the stash in a `BTreeMap`. For production, consider a
merge batcher (as upsert uses) instead.

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
