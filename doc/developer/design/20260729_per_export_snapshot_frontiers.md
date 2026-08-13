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
operator, reclock, decode and envelope, and persist sink. One thing was not. The committed upper
coming back from the persist sinks was concatenated into a single feedback edge, so its frontier
was the meet across exports and every export reported the slowest one's `offset_committed`. See
Statistics semantics.

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
  several large tables hydrate together, or whether the budget should be a replica-wide pool. Note
  the budget bounds the stash and not the builders it evicts into, so it is the real exposure only
  once the grid in step 4 lands.

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
- **Console assumes exports move in lockstep.**
  - Ingestion lag tracks the freshest export, `max(offset_committed)` over the source and its
    tables (`console/src/api/materialize/source/sourceStatistics.ts:83`, and `:142` for the
    pre-0.148 variant). Fine when every export reported the same offset, but exports can diverge
    now, so a table that is still snapshotting doesn't show up.
  - Combined sources may be overcounting. The console unions the source with its tables
    (`sourceStatistics.ts:55` and `:114`), and `mz_source_statistics` already rolls the tables up
    into the source (`src/catalog/src/builtin/mz_internal.rs:8413-8437`).
  - The snapshotting badge is all-or-nothing. It reads the source-level `snapshot_committed`
    (`console/src/api/materialize/source/sourceList.ts:71`), which is `bool_and` over the exports,
    so a source still reads as snapshotting when the rest of its exports are streaming
    (`console/src/platform/connectors/utils.ts:29-41`).
  - The per-table tab and the queries behind it only know about source status
    (`console/src/api/materialize/source/sourceTables.ts:36`,
    `console/src/platform/sources/SourceTables.tsx:90`). They should be expanded to show per-export
    info, which is where the divergence is most worth seeing.
  - `SourceDiagnostics` decides the snapshot is done from record counts instead of the
    `snapshot_committed` flag
    (`console/src/platform/maintained-objects/SourceDiagnostics.tsx:35-39`). Possibly a bug, though
    it may have been a workaround, in which case we should fix whatever made the flag unusable.
  - The maintained objects list drops anything with `sourceType == "subsource"` and shows only the
    top-level source (`console/src/platform/maintained-objects/queries.ts:160`). That is no longer
    accurate, the parent says nothing about the subsources under it.

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

There aren't any known issues related to number of input/output ports in timely

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

Sources have the time domains `F` and `T`, `FromTime` and `MzTime`, respectively.
- The snapshot operator emits data at `F:min`, which is reclocked to `T:c`, where `T:c` is the captured current time. The snapshot operator pins its capability to `F:min`, and by extension, `T:c`.
- The replication operator emits data at from-times `f >= F:min`, that are mapped to MZ times `t >= T:c`. The rewinds are currently the only data emitted by this operator at `F:min`. The replication operator downgrades caps as today.

Because snapshot has pinned its capability at `F:min`, the downgrades of the replication operator
do not move the frontier forward, so all downstream operators see events reclocked to the correct
`T` times, but the frontier does not advance. Once the snapshot is done, the operator drops its
capability, and timely propagates some time `T:n`. Left alone, the minter then emits a single
description `[T:c, T:n)` covering everything that accumulated during the snapshot. That one
description is the root of everything below. It is the only thing that tells the sink how to group,
and it arrives only after the stall it has to cover.

#### Staging

Stage arriving updates in the sink as raw rows keyed by timestamp instead of opening a
`BatchBuilder` per timestamp. An update enters a builder as soon as the description covering it is
known, which is when that description arrives rather than when it becomes ready, and the
description's bounds are wide enough for one builder to take every timestamp in its range. This
keeps the batch count proportional to the number of descriptions rather than to the number of
distinct reclocked timestamps the snapshot accumulated. Building on arrival cannot do this. The
grouping would have to be chosen before the bounds are known, and a `BatchBuilder` cannot be split
once a description boundary lands inside it.

Only updates running ahead of the descriptions stay raw, so the stash is bounded by how far the data
has run past them rather than by how long the frontier stalls. That holds only while descriptions
keep arriving during the stall, which is what the grid below is for. Under the grid the leading edge
is at most a window wide, since the minter commits `[a, a + K)` as soon as the data reaches
`a + K`.

`storage_persist_sink_max_raw_stash_bytes` bounds that edge, per worker per export. Over budget the
sink consolidates first. The pinned timestamp is where consolidation pays off, since the snapshot's
rows and the rewind retractions that supersede them cancel exactly, but only while both sides are
still raw. Once a description covers the pinned timestamp its rows are in a builder a later
retraction cannot reach, and the cancellation is left to persist compaction. Whatever consolidation
does not reclaim is evicted into single-timestamp builders. A single timestamp is safe to write
before its description exists, because a description covers a timestamp entirely or not at all.

An evicted builder holds its rows rather than its budget. A `BatchBuilder` keeps updates in a
columnar buffer and writes a part only once that buffer reaches `persist_blob_target_size`, so a few
MiB evicted into a fresh builder leaves the rows resident while the stash accounting reads zero. It
converges only where one timestamp keeps receiving data until its builder crosses the target, which
is the large transaction case below. So an evicted builder is finished the moment a description
covers it, rather than held for the readiness a stall is precisely not delivering. Both the stash
and the evicted builders are then bounded by the leading edge.

#### Committed description grid

The sink can group without waiting for the frontier if it knows one thing, where the next
description boundary falls. Have the minter commit to boundaries instead of deriving them from the
frontier. It emits `[a, a + K)` and, having emitted it, honors it: a frontier arriving at `a + K/2`
is ignored, and the next description still ends at `a + K`.

Emitting a description before its upper is complete is safe because completeness was never the
description's job. The sink appends a description only once `desired_frontier` has reached its
upper, so a committed cell waits in `in_flight_batches` until the frontier certifies it exactly as
an uncommitted one would.

The minter has to hold the upper it committed to apart from the frontier it observed. Those are one
value today: it emits `[current_upper, desired_frontier)`, downgrades to `desired_frontier`, and
carries that forward. A committed cell's upper runs ahead of the frontier, so the downgrade targets
the emitted upper instead and `current_upper > desired_frontier` becomes a state the operator has
to expect. The commitment then enforces itself, since a frontier landing at `a + K/2` fails the
`current_upper < desired_frontier` test and emits nothing. One range not to cut into cells is a
fresh export's first description: it runs from the shard minimum to `T:c`, which is epoch sized and
holds no data.

With the boundary in hand the sink routes rather than accumulates. An update whose cell has arrived
goes straight into that cell's builder, and only what runs ahead of the grid is stashed. Several
batches may go under one cell, since every batch written for a cell shares its append, so the sink
finishes a cell's builder whenever memory says to and routes later updates for that cell into a
fresh one. Nothing needs to know that a cell is complete. Resident memory is one part per open
builder plus the leading edge, independent of stall duration and of the number of timestamps, and
single-timestamp eviction falls back to what it is for, data no committed cell covers yet.

The data the minter already sees drives the commitment. It passes the desired collection through, so
during a stall it observes updates at timestamps far above a frontier that is not moving, which is
the stall's signature and needs no new input. The grid's anchor is shared for free: `T:c` is a
reclocked value from the durable remap shard, so every worker and every replica cuts at the same
points.

Two rules compose without a mode between them. Emit `[current_upper, desired_frontier)` whenever the
frontier is ahead of the committed upper, which is steady state and quantizes nothing, and emit
`[current_upper, current_upper + K)` while the largest timestamp seen is at or beyond
`current_upper + K`, which only happens while the frontier is stuck. In steady state the largest
timestamp seen stays within a tick of the committed upper, so the second rule stops firing on its
own, and a frontier arriving inside a committed cell fails the first rule and is ignored. Nothing
has to detect that the stall ended.

How deep the commitment runs is worker local, since the minter's input is `Pipeline` connected and
only the active worker mints. Within a replica that is harmless, because the minter broadcasts what
it decides. Two replicas can commit to different depths, which costs at most one straddling batch
per transition, where the winner's frontier derived upper lands inside a cell the loser had
committed. Reading the remap frontier instead of the data would not remove that, since replicas
observe it at different instants.

Freshness is charged once, at the transition. Cells below the frontier become ready and are
appended, but the cell holding the frontier cannot be, so the shard upper trails by up to `K` from
the moment the pin drops until the frontier passes the last speculative boundary. `K` bounds that
trailing rather than quantizing steady state.

`K` trades two costs against each other. Every committed cell becomes ready in the same pass when
the pin drops and appends happen one description at a time, so catching up costs `stall / K`
`compare_and_append` round trips, and an idle cell still costs one to advance the upper through it.
Parts per append is a cell's volume over the blob target. Both want `K` coarse, minutes rather than
seconds. Memory does not constrain it, because a cell may hold several batches.

The prototype implements the stash, consolidation, single-timestamp eviction, and the grid.
`storage_persist_sink_description_window` carries `K` and disables the grid at zero. No bindings
input is needed, since a cell taking several batches removes the need to certify one as complete.

#### Recovery

Batches now span multiple timestamps, which does not change the recovery path. When a concurrent
writer raises the shard upper into the middle of a description, the sink advances the description's
lower and re-appends. Persist registers a batch under the narrowed description and filters the
updates outside those bounds on read, so a batch holding data on both sides of the new lower stays
usable: the updates the concurrent writer already committed do not come back, and the ones the sink
still owes are preserved. Each batch carries the largest timestamp it holds, which is enough to
delete the batches lying entirely below the new lower rather than registering parts that would be
truncated away in full.

A batch that straddles a narrowed lower does need one thing from persist. Part `diffs_sum` is
computed from the part's raw contents and is not adjusted when the batch is registered truncated,
so compaction reads fewer diffs than the shard claims and its validation trips. Single-timestamp
batches could never straddle, which is why this only shows up now. Persist has a proposed fix in
[#38261](https://github.com/MaterializeInc/materialize/pull/38261) and the prototype has been
through CI and deployed to staging on top of it.

Persist decides a batch is truncated by comparing the bounds the builder declared against the
bounds it is appended under, not by looking at the data, and it requires the declared bounds to
contain the append bounds. A builder opened before the description covering it is known has to
declare the operator's lower, the only lower guaranteed to be at or below every description that
could cover it, and that declaration is what registers it as truncated even when every update in it
sits inside the description. Routing into a committed cell's builder avoids the marker, since the
builder then declares that cell's own lower, which leaves it to the leading edge alone. Worth doing,
because the marker exempts a run from diff sum validation, so handing it out for declarative reasons
costs real checking.

#### Statistics semantics

`updates_staged` counts on arrival rather than when an update reaches a builder, so a pinned
frontier does not make the sink look idle while it is ingesting hard. `updates_committed` counts at
append, from batch metrics that only cover updates which survived consolidation.

The two therefore relate as `updates_staged >= updates_committed`, and the gap is whatever the
stash consolidated away. Before this change the sink never consolidated, so the two matched exactly
for every workload. The gap is information rather than drift, it reports the work the sink avoided
writing. Back to back upstream transactions that reclock to the same timestamp and touch the same
row are the ordinary way to produce one.

`offset_committed` is reported per export. Each export gets its own feedback edge from its persist
sink, and one operator in the pipeline inverts each of those uppers through the remap bindings and
reports the result for that export alone. `SourceTimestamp::to_offset_stat` does the frontier to
offset conversion, which every timestamp type already had in some form on the source side. The
per-source stat sites are gone, so nothing double counts. What the source acknowledges upstream is
still the meet across exports, that has to respect the slowest one, and `mz_source_statistics`
reports the parent source as `MIN(offset_committed)` over its exports, so the source-level number
keeps its old meaning while the per-export rows tell the truth about each table.

Two things fall out of that. Every worker has to initialize the gauge, because the controller only
aggregates a gauge once every worker has reported a value for it and renders a failed aggregate as
zero, which single-worker tests will not catch. And PostgreSQL no longer pre-fills
`offset_committed` with the slot's resume LSN at startup, which existed to keep the lag calculation
from looking enormous during an initial snapshot. An export reads zero until its first commit now.
That is the honest per-export lag, but anything subtracting it from `offset_known` will show the
whole LSN as lag while an export snapshots.

#### Handling for large xacts

If an upstream transaction contains a large number of rows they all land at a single timestamp in
the stash. That is what eviction takes first, so it is written out on its own rather than held in
memory. Memory stays bounded, at the cost of one batch for that transaction.

The prototype holds the stash in a `BTreeMap`. For production, consider a merge batcher instead.

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
  Rejected as stated, because the promise was derived from the minter's frontier, which is pinned
  for exactly the duration of a snapshot. The committed grid in step 4 is the corrected form. The
  minter commits to boundaries of its own choosing instead of promising something about a frontier
  it cannot move, and the remap bindings trigger the commitment.
- **Source read progress as the coalescing horizon.**
  - Feed the source's reclocked read progress into the minter so the promise can advance while the
  collection's frontier is pinned. Rejected because a horizon that certifies completeness has to
  come from the data path to be sound. Computing it beside the data path lets it announce a time
  the reclock operator has not released updates for yet, so it needs a progress output on the
  per-export reclock, ordered behind the data that operator emits, and even then it cannot pass the
  upsert merge batcher. Allowing several batches under one description drops the requirement from
  completeness to a boundary trigger, which the bindings satisfy without touching source
  machinery.
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
