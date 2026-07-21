# Per-Export Frontiers for Source Snapshots

## The Problem

Adding a table to a running source (`CREATE TABLE ... FROM SOURCE`, or the legacy
`ALTER SOURCE ... ADD SUBSOURCE`) restarts the ingestion dataflow. On the new incarnation, the
added table has an empty resume upper and is snapshotted. Until that snapshot completes, no
subsource of the source makes visible progress. CDC events for already hydrated tables are not
processed, and their persist uppers do not advance. A snapshot of one large table can block every
other subsource for hours.

If multiple tables are added together, snapshotting may complete for the smaller tables, but they
cannot make progress until the largest table is also done. Any error during this time restarts
snapshotting for all tables.

For the rewind-pattern sources (PostgreSQL, MySQL, SQL Server) the lost progress has two
causes, both in the upstream reader operators:

1. The replication operator drains its rewind input to completion before processing any CDC
   events. Rewind requests are only sent after `COPY` (for OLTP) completes, so the CDC stream is
   not read while a snapshot runs.
2. All exports share a single data capability. While any rewind request is pending, the capability
   cannot be downgraded past the minimum offset, because the operator must still be able to emit
   retractions at the minimum timestamp.

Kafka blocks for a different reason. Multiple tables can also be created from one Kafka source, for
example to accommodate schema changes in the topic. The reader resumes every partition from the
minimum resume upper across all exports, so adding a table makes the single consumer re-read the
topic from the new table's start offsets, and the shared output port means no export commits new
data until the re-read reaches the tip.

Everything downstream of the snapshot and replication operators is already per export.
- the statistics operator
- reclock
- decode and envelope
- the persist sink

One thing was not. The shard upper coming back from each persist sink was concatenated into a
single feedback edge, so its frontier was the meet across exports and every export reported the
slowest one's `offset_committed`. See Statistics semantics.

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
  from a source that does not already have them.
- Memory on the replica during hydration is bounded by the unflushed tail of each open builder,
  independent of snapshot duration, as long as the minter keeps committing windows. A minter that
  sees none of an export's data breaks that bound, see Open Questions.
- The number of batches the sink appends follows the number of committed windows plus the boundary
  trailblazers, so it grows with the logarithm of the snapshot's duration and then linearly, one
  per capped window, not with the timestamps the snapshot accumulated.
- A restart during a snapshot remains correct. The snapshot restarts from the beginning, as today.


## Solution Proposal

Give each export its own output port and capability on the snapshot/replication operators, and
hold back only the hydrating exports. Sources that use the rewind pattern keep their protocol
unchanged. Kafka would need no reconciliation at all, a backfill feed and a steady feed read
disjoint offset ranges, but its second consumer is deferred, see the Kafka section and Rollout.

The work is confined to each source. `SourceRender::render` already returns per-export
collections, reclock derives per-export output frontiers from per-export input frontiers without
changes, and remap bindings are minted from the probe channel independent of data capabilities.

Steps 1 and 4 apply to all sources. Steps 2 and 3 are the rewind-pattern changes. The Kafka
section sketches their Kafka equivalent for the deferred work.

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

There aren't any known issues with the number of input or output ports in timely.

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
minimum until its snapshot completes, the `COPY` for PostgreSQL, then closes.

**Replication operator:** each port of hydrated exports downgrades with the data upper. A
hydrating export's port holds at the minimum until the data upper passes its snapshot offset,
because events at or below the snapshot offset must still be emitted negated at the minimum for
that export. Once the upper passes the snapshot offset, the port downgrades with the data upper
like the rest. The emission of rewind negations remains unchanged.

Sources still acknowledge at the minimum shard upper across exports. For PG, a hydrating
export continues to pin WAL retention, but no longer pins any other export's frontier.

### 4. Bounded sink accumulation

A batch's bounds come from a batch description, which `mint_batch_descriptions` emits when the
collection's frontier advances. While an export snapshots its frontier is pinned, so no
description is minted for the duration. The sink still receives snapshot rows at the pinned time
and CDC events at later times, and has to group them into batches without knowing which
description will cover them.

Sources have the time domains `F` and `T`, `FromTime` and `MzTime`, respectively. The generic type
names are `FromTime` and `IntoTime`, and in practice `IntoTime` is MZ time.
- The snapshot operator emits data at `F:min`, which is reclocked to `T:c`, where `T:c` is the
  captured current time. The snapshot operator pins its capability to `F:min`, and by extension,
  `T:c`.
- The replication operator emits data at from-times `f >= F:min`, that are mapped to MZ times
  `t >= T:c`. The rewinds are currently the only data emitted by this operator at `F:min`. The
  replication operator downgrades caps as today.

Because snapshot has pinned its capability at `F:min`, the downgrades of the replication operator
do not move the frontier forward, so all downstream operators see events reclocked to the correct
`T` times. Once the snapshot is done, the operator drops its capability, and timely propagates some
time `T:n`. Left to the frontier alone, the minter emits a single description `[T:c, T:n)`
covering everything that accumulated during the snapshot. That one description is the root of the
problem the rest of this step solves. It is the only thing that tells the sink how to group, and it
arrives only after the snapshot it has to cover.

#### Grouping

The sink writes one batch per description, taking every timestamp that description covers into a
single `BatchBuilder`. This keeps the batch count proportional to the number of descriptions rather
than to the number of distinct reclocked timestamps the snapshot accumulated. It needs the bounds
first. The grouping cannot be chosen before they are known, and a `BatchBuilder` cannot be split
once a description boundary lands inside it.

An update whose description has not arrived writes a batch of its own timestamp. A single timestamp
is safe to write without knowing the descriptions, because a description covers a timestamp
entirely or not at all, and it is exactly what the sink does for every update today. It declares
the operator's lower, the only lower guaranteed to be at or below every description that could come
to cover it, and is finished the moment a description covers it.

The data that arrives before the description covering it is the leading edge. The descriptions
below the leading edge are committed to by the minter ahead of the data, so the leading edge is the
rows that reach a writer at a window boundary before the broadcast description does, and everything
else is grouped. A row in the leading edge is a trailblazer, ahead of its description.

An update goes into a `BatchBuilder` as it arrives. The builder is the buffer. It writes a part to
blob every `persist_blob_target_size` and keeps at most
`persist_batch_builder_max_outstanding_parts` uploads in flight. What sits in memory while the
frontier is pinned is the unflushed tail of each open builder. A batch stays unlinked in blob until
its description is appended, so the sink trades memory for blob writes it may throw away on a
restart.

Two kinds of builder are open at once.
- A builder per in-flight description, taking data at timestamps in its range.
- A builder per trailblazer timestamp, for data in the leading edge that no description covers
  yet.

A description's builder closes as soon as data moves past it. This keeps their number from following
how long the frontier stays pinned, with one exception. The builder whose description covers the
frontier's own time stays open until that description is ready. The frontier's time is incomplete by
definition, and during a snapshot it is where the snapshot's rows land for the snapshot's whole
duration, interleaved with replication rows that have moved on to later windows. Closing that
builder whenever a later window's row arrives reopens it on the next snapshot row, one batch per
flip.

Arrival order is an efficiency assumption, not a correctness one. Readiness and the
`operator_batch_lower` declared by trailblazer data are both gated on the frontier, so a straggler,
a row arriving for a window whose builder has already closed, reopens that builder and costs one
extra batch.

#### Committed descriptions

The sink can group without waiting for the frontier if it knows where the next description boundary
falls. The minter will "commit" to boundaries instead of deriving them. I'm not sure if there's a
better word for this, but this is the upper that the minter "commits" to uphold. It emits `[a, b)`
for a `b` of its own choosing and then honors it. So a frontier arriving inside `[a, b)` is ignored,
and the next description still starts at `b`. We choose a `b` that is larger than the frontier
time step, so eventually this collapses when the snapshot completes and the next frontier arrives.
The minter emits `[b, frontier_upper)`, and from that point forward, the persist sink operates
as it does today.

Committing to a boundary for a description before the frontier advances is safe because the
description doesn't dictate completeness. The sink appends to persist only once `desired_frontier`
reaches its upper, so a "committed" description waits in `in_flight_batches` until then.

To ensure we can start grouping immediately, the minter commits to a boundary ahead of the data. A
description has to reach the writers before the rows it covers, since a builder only takes rows at
times it was opened for. So the first window `[T:c, T:c + w)` is committed the moment the export's
frontier pins, before any row has arrived, on the knowledge that the snapshot's rows are about to
land at `T:c` itself. The minter passes data for the desired collection through, so it sees the
largest timestamp the data has reached. It uses this to determine the boundary for the next window,
once that timestamp comes within a margin of the committed upper. The margin is equal to the
initial width, the dyncfg value, so the description still has a head start over the rows. Each
commitment doubles the width of the next, up to `storage_persist_sink_description_window_max`, so a
short snapshot commits little past its end and a long one costs few descriptions.

The minter has two rules for emitting a description.
- Emit `[current_upper, desired_frontier)` whenever the frontier is ahead of the committed upper,
  which is steady state and commits to nothing.
- Emit `[current_upper, current_upper + width)` once `max_seen + margin > current_upper`, while the
  export's snapshot is in progress.

Only a commitment ending past the frontier widens the window, so the doubling is driven by the
pinned frontier during snapshot, not by steady state. Committing costs the minter the coupling
between the upper it emits and the frontier it observes. The minter downgrades to that upper
rather than to the frontier, so `current_upper > desired_frontier` becomes a state it has to expect.

The two rules interleave in three phases. `F` is the frontier and `C` the committed upper, with the
snapshot pinned at `c` and an initial width of `w`.

While the snapshot runs, `F` does not move and `C` steps ahead of the data, each step on the second
rule:

```
frontier     F                                      C        F pinned at c for the whole snapshot
MZ time  ----c------c+w---------c+3w----------------c+7w-->
             [c,c+w)[c+w,  c+3w)[c+3w,        c+7w)          C steps ahead of the data
             on the when data   when data
             pin    nears c+w   nears c+3w
```

When the snapshot ends, `F` jumps to wherever the source has reached. Every window below it appends
in one pass. The window it lands in binds, and the distance from `F` to `C` is the tail:

```
frontier                                    F       C        F jumps to where the source is
MZ time  ----c------------------c+3w----------------c+7w-->
             [c, c+3w) appended [c+3w, c+7w) binds           shard upper waits at c+3w
                                            |-tail->|        until F reaches c+7w
```

Once `F` passes `C`, the first rule takes over and every description is derived from the frontier,
so `C` and `F` coincide from then on:

```
frontier                FC        FC        FC               C rides on F
MZ time  ----c+7w-------F1--------F2--------F3------------>
             [c+7w, F1) [F1, F2)  [F2, F3)                   each derived from the frontier
```

An export snapshots when its resume upper is the minimum from-time, and this is passed into the
persist sink. Exports with the CDCv2 envelope are excluded. Their MZ times come from the data rather
than from reclocking, so a wall-clock width has no meaning there and a committed upper the data
never reaches would hold the shard upper forever. The test has to be made in the from-time domain.
Reclocking a resume upper maps any MZ time at or below the as_of back to the minimum, which is what
keeps a restart during a snapshot reading as snapshotting even though its shard upper has moved past
`T:min`. The frontier alone doesn't carry enough information to determine if a snapshot is
happening, as any dataflow restart would appear to be snapshot.

The frontier is used to determine when the snapshot ends. This relies on the invariant that a
snapshot occupies a single MZ time. Sources that rewind emit theirs at `F:min`, so it reclocks to
`T:c`, which the frontier holds until the snapshot port closes and the replication port downgrades
past the snapshot offset. Kafka reads real offsets rather than `F:min` and reaches the same place,
see the Kafka section. The minter keeps the first non-minimum frontier a snapshotting export takes
and permits the second rule only while `desired_frontier` equals it, and never before that frontier
has arrived. Timely does not order progress ahead of data, and a row can reach the minter under a
frontier still at the minimum. Committing on it would anchor the window at the shard upper rather
than at `T:c` and mint windows across the whole gap between them, which on a fresh shard is every
window between zero and the wall clock. Once a snapshot is committed and the shard upper advances,
this path is not reachable for the export.

Catching up costs one append. Every committed description becomes ready in the same pass when the
frontier advances, and `append_batches` combines every description ready in a pass into a single
`compare_and_append` over the whole range. Setting `persist_validate_part_bounds_on_write` or
`persist_validate_part_bounds_on_read` gives each description an append of its own instead.

The commitment's cost is a tail. A commitment is binding, so while `current_upper` sits ahead of the
frontier the first rule mints nothing, and when the snapshot ends the shard upper waits for the
frontier to reach the last committed upper rather than advancing to where the frontier actually
reached. The last window was committed when the data came within the margin of the previous upper,
so the tail is at most the margin plus the last width, which with doubling is up to the snapshot's
own duration, capped by `storage_persist_sink_description_window_max`. The snapshot gate confines
the exposure to exports that are snapshotting, where the frontier is not advancing anywhere in the
first place. A collection keeping up lags the data by about one `timestamp_interval` and has nothing
to group, so committing there would pay the tail for nothing. An export whose stream is quiet during
its snapshot and then receives a burst commits every window between the last upper and the burst in
one pass, each an empty description that still has to be minted, broadcast, and carried to the
append.

Leaving the window at zero mints descriptions from the frontier alone, so the sink writes one batch
per timestamp exactly as it does today.

#### Commit timing across workers

Only the active worker mints, and its input is `Pipeline` connected, so the timestamp it times the
next commitment against is the largest one reaching its own sink instance. Descriptions travel to
the writers by broadcast while data reaches each writer directly, so another worker can take in a
row at a window boundary before the description covering it arrives. The margin is the head start
that closes that gap, and a row that beats its description anyway is a trailblazer. It writes a
batch of its own timestamp, finished under the description's bounds when it lands.

How much of an export the minting worker sees varies by source. PostgreSQL round robins
replication rows across all workers, so every worker's largest timestamp tracks the stream and the
margin is enough. MySQL reads its binlog on one worker. A minter that is not that worker sees none
of the CDC, so its largest timestamp never moves past `T:c`, no window after the first is
committed, and everything the binlog worker takes in during the snapshot degrades to one batch per
timestamp. That shape needs either each worker exchanging its largest timestamp to the minter on a
disconnected input, or a trigger that does not depend on data at all, see Open Questions.

Replicas commit independently, so two of them land on different boundaries. That costs at most one
straddling batch per transition, which the append path already trims against a raised shard upper.

#### Concurrent ingestion

A concurrent writer, another replica of the same ingestion, can raise the shard upper into the
middle of a description. The sink advances that description's lower and re-appends. Persist
registers the batch under the narrowed bounds, as a truncated batch, and filters the out-of-bounds
updates on read. Batches carry their largest timestamp, so ones lying entirely below the new lower
are deleted entirely rather than appended as truncated.

#### Statistics semantics

`offset_committed` is reported per export. Each export gets its own feedback edge carrying its
shard upper back from its persist sink, and one operator in the pipeline inverts each of those
uppers through the remap bindings and reports the result for that export alone.
`SourceTimestamp::to_offset_stat` does the frontier to offset conversion, which every timestamp
type already had in some form on the source side. What the source acknowledges upstream is still
the meet across exports, that has to respect the slowest one, and `mz_source_statistics` reports the
parent source as `MIN(offset_committed)` over its exports, so the source-level number keeps its old
meaning while the per-export rows tell the truth about each
table.

Two things fall out of that. Every worker has to initialize the gauge, because the controller only
aggregates a gauge once every worker has reported a value for it and renders a failed aggregate as
zero, which single-worker tests will not catch. And PostgreSQL no longer pre-fills
`offset_committed` with the slot's resume LSN at startup, which existed to keep the lag calculation
from looking enormous during an initial snapshot. An export reads zero until its first commit now.
That is the honest per-export lag, but anything subtracting it from `offset_known` will show the
whole LSN as lag while an export snapshots.

#### Handling for large xacts

If an upstream transaction contains a large number of rows they all land at a single timestamp. The
builder holding them spills its parts to blob once it passes `persist_blob_target_size`, so memory
is bounded by that rather than by the transaction. A description covering that timestamp puts the
transaction in the same batch, and the same append, as everything else the description covers.

### Kafka backfill consumer with offset handoff, deferred

This section is the sketch for the Kafka side, which Rollout defers. Kafka has no rewind machinery.
The "snapshot" of a new export is a re-read of the topic from the export's start offsets, and "CDC"
events are the data after the snapshot, split by Kafka offset. If there are two exports for the same
topic, the hydrated export stops progressing because the shared Kafka consumer has to read from the
other export's start offsets.

The Kafka source would have up to two consumers, one backfilling every export that needs hydration
and one for steady state. If no hydration is necessary, or all exports need hydration, only a single
consumer is created. This limits the two consumer case to the situation that motivates it, adding a
new export where one already exists.

For the mixed export case, a frontier `B` is determined on startup. The backfill consumer will
capture rows below `B`, and the steady state consumer captures rows at or above `B`. The two
consumers emit concurrently for hydrating exports. As today with transition from hydration to
steady state, the handoff at frontier `B` requires no dedup or negation. This is possible because
the set of records for the snapshot and the set of records for steady state are disjoint. The
logical choice for `B` is the minimum `resume_upper` for all non-hydrating exports in the mixed
export case. For the case where all exports are hydrating, `B` keeps its existing definition,
which is the maximum frontier (maximum offsets of each partition).

The backfill occupies a single MZ time, which is what the sink's snapshot gate reads. Reclocking
maps an offset to the earliest binding whose source upper exceeds it, and the controller holds the
remap shard's since one step behind the exports' uppers, so the trace a restart reads is compacted
to `T:c` and every offset below the source upper there lands on it. `B` sits within a step of that
bound, so the hydrating export's frontier stays at `T:c` for the backfill's duration and advances
once when the backfill port closes. A source where every export is hydrating gets there by a
different route. The first binding minted into an empty remap shard maps the probed tip to the
minimum MZ time.

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

Upsert v2 is rolling out and is expected to become the default. Correctness holds there, since
snapshot and steady state records land in the merge batcher and are only emitted on frontier
progression. Unfortunately, updates will accumulate in the upsert merge batcher because the frontier
is pinned, increasing memory utilization. The increase is the replication updates (e.g. messages 
at offsets post-snapshot). The batcher pages cold data out of resident memory, but the staged
volume itself grows with the snapshot duration. Additionally, the merge batcher drain is sequential
per timestamp. For a long hydration period, this drain would cover thousands of timestamps, each
making a round-trip through persist. Upsert needs rework to cover the concurrent streams behind
a pinned frontier. This is deferred as it needs more thought and adds a lot more risk around
correctness, so is best handled as a separate effort.

### Restart semantics

A restart during hydration discards the accumulated updates. That now includes the CDC staged
during the snapshot, where before there was only snapshot progress to lose. Multiple tables
snapshotting together come out ahead. Today none of them can complete until all do, so a restart
sends every table back to the start. With independent frontiers a table that finished keeps its
snapshot and only the unfinished ones start over.

The upstream position the source acknowledges is the meet over exports of their shard uppers,
inverted through the remap bindings. A snapshotting export's shard upper never passes `T:c`, so the
meet stays at the position the source resumed from when the export was added, `P`, and the upstream
retains everything from `P` for the whole snapshot. For PostgreSQL that position is the replication
slot's confirmed flush LSN. And the dataflow `as_of` on restart is the remap since, which the
snapshotting export's read hold keeps at `T:c`, so the new incarnation reclocks that export's shard
upper back to the minimum and snapshots it again even though the empty first description moved the
upper to `T:c`.

These are notes on the newly possible states and how to test them. The cases below use a hydrated
export `A`, a new table `B` with snapshot offset `s`, and `B`'s appended windows written as
`[c, c+kw)`. Offsets are the source's from-time, LSNs for PostgreSQL.

1. **Restart during the copy.** `B` snapshots again at a new snapshot offset `s' > s`, stamped
   at the same `T:c`. The resume offset is the minimum over exports and `B` contributes `P`, so
   the stream restarts at `P` and re-decodes everything `A` appended during the snapshot. Those
   rows are dropped below `A`'s shard upper, but `A`'s frontier and its `offset_committed` hold
   until the re-read passes `A`'s resume offset. The restart also reads the
   remap trace retained since `T:c`, one binding per probe for the snapshot's duration, in both
   `reclock_resume_uppers` and `reclock_committed_upper`. Correctness rests on the filtering of
   re-read rows below an export's upper in the persist sink and in upsert.
2. **Restart in the tail.** The copy is complete, `B`'s frontier sits inside the last committed
   window, and the windows below it are appended, so `B`'s shard upper is a window boundary such as
   `c+3w`. When `s` reclocks to a time above that boundary, `B`'s resume offset is below its own
   snapshot offset, yet `B` counts as hydrated, so no copy and no rewind request. `B`'s events
   between the resume offset and `s` are emitted forward at times at or above `c+3w`, while their
   negations at `T:c` are already in the appended batch. The appended prefix is `B`'s state at the
   resume offset, so the result is correct. Correctness rests on appends being frontier gated,
   which makes any `B` upper past `T:c` imply a complete snapshot.
3. **Two tables added together, one completes first.** `B1` is hydrated and advancing while `B2`
   still copies. Only `B2` snapshots again. `B1` resumes from its own upper and keeps its data, as
   does `A`. The stream still restarts at `P` because `B2` contributes it, so `B1`'s events between
   `P` and its resume offset arrive again and are dropped, and `B2`'s rewind negates `(P, s2']`.
4. **Restart after `B`'s replication port releases but before the copy ends.** The stream has
   passed `s`, the rewind entry is gone, and `B`'s forward events already flow into windows.
   Nothing is appended past `T:c`, so the restart is case 1, but the negations emitted for `(P, s]`
   are discarded with the batches and the new incarnation negates the wider `(P, s']`. The leaked
   batches hold that CDC volume as well as the snapshot, see Out of Scope.
5. **Drop of the snapshotting table.** Dropping `B` restarts the dataflow without it. Its read
   hold goes, the remap since moves up to `A`'s upper, the resume offset becomes `A`'s, and the
   acknowledged position advances from `P`. Dropping `B1` while `B2` still copies must leave it at
   `P`, since `B2` still contributes it.

Cases 1 and 2 are the newly possible states. A stream restarting at `P` while siblings are far
ahead, and a hydrated resume point below the export's own snapshot offset. Cases 3 to 5 are
existing behavior over wider intervals. Each needs a test that restarts the replica in that state
and compares every export against upstream, see Open Questions.

### Rollout

PostgreSQL first, then MySQL and SQL Server, which use the same rewind structure adapted to their
offset types. MySQL keys `RewindRequest` by a GTID snapshot upper, SQL Server already tracks a
per-export `initial_lsn` and `snapshot_lsn` pair. In both, the snapshot upper is available when
the snapshot transaction is established, so the same three operator changes apply. Kafka is
deferred, since its backfill consumer is new machinery and upsert needs a rethink.

The behavior change is gated by the
`storage_source_snapshot_concurrent_replication` feature flag, default off in production and
default on in CI so the new path is exercised by the test suites before it is enabled.

`storage_persist_sink_description_window` is separate and defaults to zero, which leaves the sink
writing one batch per timestamp. The test configuration sets it to one second with
`storage_persist_sink_description_window_max` at five minutes. Concurrent replication is what makes
a snapshot accumulate many timestamps, so the window has to carry a value before that flag is turned
on.

In general, a source supports independent export frontiers when it can do four things.
1. emit each export on its own output port with its own capabilities
2. determine the snapshot consistency bound when the snapshot starts rather than when it completes
3. reconcile snapshot and stream either by compensating negated emission at the minimum time or
   through an envelope that derives retractions from state
4. derive per-export progress from its own read process


## Open Questions

- **Restart interleavings.**
  - Which of the five cases under Restart semantics get a test, and in which framework (platform
    checks, testdrive). Case 2 needs a window small enough that the first appended window closes
    before the snapshot offset's time. Cases 1 and 4 need a replica killed while a copy runs against
    a busy sibling.
- **Trailblazer visibility.**
  - Whether the leading edge needs a metric for the batches it produces, so a snapshot whose
    trailblazers, or whose blind minter, degrade it toward one batch per timestamp is visible rather
    than inferred from persist shard shape.
- **Reader distribution versus pipelining.**
  - Whether source readers should round robin rows to all workers, as PostgreSQL does, or whether
    an export should stay on the worker that read it. The source implementations already disagree.
    PostgreSQL exchanges and casts rows downstream of the reader, MySQL reads, decodes and emits on
    one worker. Neither shape is known to be the faster one, there is no data.
  - Pipelining is less complex, and part of what would bound it is persist part uploads. Uploads
    already run as spawned tasks, but `persist_batch_builder_max_outstanding_parts` defaults to 2,
    so the builder blocks on blob storage long before the append that actually commits, and
    `write_stalls` counts it when that happens. Measuring a single export at high volume on one
    worker, with `write_stalls` and CPU separated, would show where the bottlenecks are.
  - The one-worker shape is also where the minter goes blind, so the answer decides whether each
    worker exchanges its largest timestamp to the minter, one timestamp rather than a byte count,
    or whether the trigger moves to the wall clock, which MZ time is in milliseconds.
- **Multiple concurrent hydrations.**
  - Builders spill to blob at `persist_blob_target_size`, so the resident exposure per export is one
    unflushed part for the builder covering the frontier's time, one for the current window, and one
    per trailblazer timestamp, summed over every export snapshotting at once. Whether that stays
    inside a replica's memory when several large tables hydrate together.
- **Tail after long snapshots.**
  - A snapshot longer than the window cap ends with a tail of up to the cap plus the margin before
    its export is readable. Whether that is acceptable, and what a policy that shortens the last
    window as the snapshot nears its end would cost in descriptions, given the sink cannot see the
    end coming.


## Cross-component impact (Claude discovered)

- **Console assumes exports move in lockstep.**
  - Ingestion lag tracks the freshest export, `max(offset_committed)` over the source and its
    tables, in `console/src/api/materialize/source/sourceStatistics.ts` for both the current query
    and the pre-0.148 variant. Fine when every export reported the same offset, but exports can
    diverge now, so a table that is still snapshotting doesn't show up.
  - Combined sources may be overcounting. The console unions the source with its tables in
    `sourceStatistics.ts`, and `mz_source_statistics` in `src/catalog/src/builtin/mz_internal.rs`
    already rolls the tables up into the source.
  - The snapshotting badge is all-or-nothing. `console/src/api/materialize/source/sourceList.ts`
    reads the source-level `snapshot_committed`, which is `bool_and` over the exports, so
    `console/src/platform/connectors/utils.ts` shows a source as snapshotting while the rest of its
    exports are streaming.
  - The per-table tab and the queries behind it only know about source status, in
    `console/src/api/materialize/source/sourceTables.ts` and
    `console/src/platform/sources/SourceTables.tsx`. They should be expanded to show per-export
    info, which is where the divergence is most worth seeing.
  - `console/src/platform/maintained-objects/SourceDiagnostics.tsx` decides the snapshot is done
    from record counts instead of the `snapshot_committed` flag. Possibly a bug, though it may have
    been a workaround, in which case we should fix whatever made the flag unusable.
  - The maintained objects list in `console/src/platform/maintained-objects/queries.ts` drops
    anything with `sourceType == "subsource"` and shows only the top-level source. That is no
    longer accurate, the parent says nothing about the subsources under it.


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
  - The deferred Kafka design relies on two consumers, a backfill and a steady state one. In a
    mixed-export scenario, some exports hydrating and some in steady state, the hydrating
    exports' frontier is pinned due to hydration. Records from both the snapshot and steady state
    will collect in the upsert's merge batcher, where before it was only snapshot records.
    Addressing this requires additional design around upsert.


## Considered but Rejected

Summary of the rejected options and why.

- **DBLog-style watermark snapshots.**
  - Interleaving chunked snapshot reads with the stream, deduplicating by primary key between
  watermarks, produces a per-key upsert stream with no consistent point-in-time state. Rejected
  because storage collections require exact multiplicities, the nondeterministic interleaving
  violates definiteness, and all sources would now require upsert.
- **Persist extensions as the primary mechanism.**
  - Gap appends or placeholder batches would make concurrent CDC durable immediately and remove the
  sink accumulation problem entirely, and a staged batch registry would make the grouped builders
  crash-safe. Rejected for the initial delivery because they change persist shard state, compaction,
  and read paths.
- **Out-of-band snapshot committed directly to persist.**
  - Run the snapshot as a one-shot job and `compare_and_append` the result, then restart the main
  dataflow to pick up the export, which is now past the snapshot, so no other exports are affected.
  Rejected because it needs infrastructure this design does not.
    - a one-shot job outside the ingestion dataflow
    - a durable handoff record
    - verification that the slot has not compacted past the recorded LSN
    - per-export start points threaded into the dataflow
- **Shadow pipeline with catch-up and handoff.**
  - A second temporary pipeline with its own replication slot snapshots the new table, follows CDC
  to an agreed LSN, and hands off. Rejected as this does not improve slot's WAL retention, adds
  connection cost on the upstream database, and requires an exactly-once handoff protocol.

The following were considered for persist sink, and all share a root cause. Each tries to
choose a batch's grouping before its bounds are known, which is the thing that cannot be done
safely.

- **Trailing-lag grouping.**
  - Hold arriving rows and move them into a grouped builder once they fall a fixed lag `L` behind
  the latest data time, rotating that builder when the frontier advances. Rejected because `L` must
  exceed the frontier's propagation delay, which is not a quantity the sink can bound, and
  exceeding it is only detected at rotation, where the recovery is a dataflow restart.
- **A coalescing horizon published by the minter.**
  - Have `mint_batch_descriptions` broadcast a promise that no future description will end below
  some time, so write operators can group updates below it before their description exists.
  Rejected as stated, because the promise was derived from the minter's frontier, which is pinned
  for exactly the duration of a snapshot. The committed descriptions in step 4 are the corrected
  form. The minter commits to boundaries of its own choosing instead of promising something about a
  frontier it cannot move, and the data it already passes through triggers the commitment.
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
- **Ending descriptions at the data, with a raw stash for the leading edge.**
  - Commit `[current_upper, max_seen + 1)` every `N` bytes the minter passes through, so a
  description ends at the data instead of ahead of it, and have each writer hold rows for
  timestamps no description covers yet as raw rows, draining them into the description's builder
  when it arrives and evicting the heaviest timestamps into single-timestamp builders when the
  stash overflows. This has real advantages. No tail, since the shard upper lands exactly where the
  frontier does when the snapshot ends, boundaries that track volume rather than time, no need to
  detect the pin, and snapshot rows that cancel against their rewind retractions before anything is
  written. Rejected because a description minted behind the data can only group rows the writer is
  still holding, so the stash is load bearing and needs a budget of its own, kept above the
  description budget so commitments drain it before eviction fires. The eviction backstop reclaims
  that budget and not memory, because an evicted builder below `persist_blob_target_size` keeps its
  rows resident, so a stream spread across many timestamps is invisible to it. And the minter sees
  only its own worker's bytes, so the trigger needs every worker to exchange running byte totals to
  it. Scaling the budget by the worker count instead assumes a source spreads an export evenly,
  which holds for PostgreSQL and not for MySQL, and the sink cannot tell the two shapes apart. That
  is two budgets with an ordering constraint, a consolidation heuristic, an eviction policy, and an
  exchange, and it still needs the same rule for the builder covering the frontier's time.
- **Pager-backed spill for the leading edge.**
  - Spill rows into `mz_ore::pager` chunks rather than into persist batches, keeping the batch count
  at one per description regardless of budget. Rejected for now because the pager's default backend
  is swap, which is ordinary heap plus a reclaim hint and so does not change the bound. Only the
  file backend does, and it depends on a scratch directory and on a process-global setting that
  compute owns.
- **Product timestamps to expose stream progress under the pin.**
  - Give the ingestion an `(outer, inner)` time so the snapshot sits at `(min, min)` and the stream
  at `(f, min)`, with the inner coordinate gating persist. Rejected because `(min, min)` is the
  minimum of the product domain and is below every stream time, so the frontier is the single
  element `{(min, min)}` for the whole snapshot and carries no more than the plain frontier does.
  Exposing stream progress needs the snapshot to be incomparable with the stream, that is above it
  in the gate coordinate. That form would give the minter a global trigger and the writer a
  frontier-backed closing rule, but it leaves the persist upper and the bounds-at-add-time trade
  untouched, and it is sound only where addition commutes, so not under upsert, where base before
  delta is exactly the order that pins.
- **Folding concurrent CDC into the snapshot time.**
  - Write the stream's rows at `T:c` alongside the snapshot for as long as it runs, so the whole
  snapshot period is one persist time, one spilling builder, no grouping, and upsert state that
  collapses per key. Rejected because the export's contents at `T:c` become the state at the offset
  where the fold ended, so its history over the interval up to that offset's time disagrees with the
  remap bindings and with the source's other exports. A dataflow installed on the export during the
  snapshot picks an `as_of` in that interval, observes the disagreement, and holds the since there
  so it cannot be advanced past.
