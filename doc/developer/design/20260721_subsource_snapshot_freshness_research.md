# Subsource Snapshot Freshness: Options Summary

This document summarizes options for allowing a newly added subsource or
source-fed table to snapshot without degrading the freshness of the other
subsources of the same source.

## The Problem

Adding a table to a running source (`CREATE TABLE ... FROM SOURCE`, or the
legacy `ALTER SOURCE ... ADD SUBSOURCE`) restarts the ingestion dataflow. On
the new incarnation, the added table has an empty resume upper and is
snapshotted. Until that snapshot completes, no subsource of the source makes
visible progress. CDC events for already hydrated tables are not processed,
and their persist uppers do not advance. A snapshot of one large table can
stall every other subsource for hours.

In a similar vein, if multiple tables are added together, snapshotting may complete for the smaller
tables, but they cannot make any progress until the largest table is also done. Any error during
this time causes snapshotting to restart for all tables.

## Background: current implementation (PostgreSQL)

The mechanism below is implemented in
`src/storage/src/source/postgres/snapshot.rs` and
`src/storage/src/source/postgres/replication.rs`. MySQL
(`src/storage/src/source/mysql/replication.rs`) and SQL Server
(`src/storage/src/source/sql_server/replication.rs`) use the same structure,
adapted to their offset types.

**Dataflow restart.** The export set is fixed when the dataflow is rendered.
There is no path to add an export to a running dataflow, so adding a table
tears down and re-renders the ingestion dataflow. Tables whose resume upper
is above the minimum are skipped by the snapshot operator, tables at the
minimum are selected for snapshot.

**Snapshot consistency.** The snapshot leader creates a temporary logical
replication slot with `USE_SNAPSHOT` inside a `REPEATABLE READ` read-only
transaction. The slot's consistent point determines the snapshot LSN. The
transaction snapshot is exported with `pg_export_snapshot()` and imported by
the other workers with `SET TRANSACTION SNAPSHOT`, so all workers `COPY` at
the same visibility. The leader's transaction stays open until all workers
finish.

**Rewind protocol.** Snapshot data is emitted at the minimum offset. For each
snapshotted table, after `COPY` completes, the snapshot operator sends a
`RewindRequest { output_index, snapshot_lsn }` to the replication operator.
The replication operator re-emits every replication event with
`commit_lsn <= snapshot_lsn` a second time, negated, at the minimum offset.
The snapshot state at `snapshot_lsn` minus the negated events between the
slot's consistent point and `snapshot_lsn` equals the state at the slot's
consistent point, so replaying the slot from there yields exact
multiplicities.

**Why other subsources stall.** The replication operator has two gates:

1. The replication operator drains its rewind input to completion before it
   opens the replication stream. The rewind input only closes after the
   snapshot operator finishes `COPY` and drops its rewind capability. This
   serialization is deliberate, to avoid staging replication data in memory
   while the snapshot runs. During the snapshot the replication stream is not
   read at all.
2. All exports share a single data capability. While any rewind request is
   pending, the capability cannot be downgraded past the minimum offset,
   because the operator must still be able to emit negated data at the
   minimum. One hydrating export therefore pins the frontier of every export
   of the source.

**Resume LSN regression.** The replication resume point is the minimum
across all exports. A newly added export resumes from the minimum, so the
stream restarts from the slot's `confirmed_flush_lsn` and replays the gap
below the existing exports' committed uppers before any new data flows.

## Constraints

**Definiteness.** The contents of a collection at each readable timestamp
must be a deterministic function of durable shared facts, namely the remap
shard contents and the upstream history. Replicas and restarts must produce
identical readable state. The per-incarnation snapshot LSN differs across
replicas and restarts, so it must not be observable in the output. The
negation-based rewind satisfies this. Filtering events against a
replica-local snapshot LSN does not. A snapshot LSN that is durably recorded
before any output depends on it is a shared fact, and filtering against such
a durably committed LSN is deterministic.

**Persist immutability.** Once a timestamp is readable in a shard, its
contents are immutable. No design may reveal partial hydration state and
amend it later.

**Upstream privileges.** The PostgreSQL source requires only `SELECT` on the
replicated tables and the `REPLICATION` attribute. No design may require
write access to the upstream database without changing this contract.

**Exact multiplicities.** Storage collections are differential. Every update
is a `(row, time, diff)` triple, and the collection at each time is the sum
of diffs. Correct retraction of a row requires knowing the row's prior value.

## Options

### A. Per-export capabilities with an early snapshot bound

Replace the single shared data capability with per-export capabilities, and
split the rewind protocol in two: the snapshot operator announces
`SnapshotStarted { export, snapshot_lsn }` when the snapshot transaction
opens (the LSN is known at that point), and completion separately after
`COPY`. The replication operator starts streaming immediately. For a
hydrating export it emits events both at their commit LSN and negated at the
minimum (for events at or below the snapshot LSN), holding back only that
export's capability. Other exports' capabilities advance freely.

Properties:

- Preserves the existing negation-based definiteness argument. No new
  durable state.
- Requires capability and rewind bookkeeping changes in each connector
  (PostgreSQL, MySQL, SQL Server).
- The hydrating export's concurrent CDC accumulates in the persist sink at
  fine-grained reclocked times while its frontier is pinned. Batches cannot
  be appended until the frontier advances. See "Sink staging to blob
  storage" below for the accumulation mechanism and mitigations.
- A restart during hydration discards the accumulated work and the snapshot
  restarts from the beginning.
- The resume LSN regression remains, since the hydrating export still needs
  the stream from the slot's consistent point.

### B. Out-of-band snapshot committed directly to persist

Run the snapshot outside the ingestion dataflow as a one-shot job: open the
temporary slot, obtain snapshot LSN `S`, `COPY`, and `compare_and_append`
the result into the new export's shard from the empty upper at the reclocked
time of `S`. The main dataflow then includes the export with a per-export
start point, ignoring events at or below `S` for that export. The SQL Server
source already implements per-export event filtering against a per-export
initial LSN.

Properties:

- The running replication stream and the other exports' frontiers are not
  involved in the snapshot.
- No rewind machinery for the added table, and no sink-side accumulation
  during hydration.
- `compare_and_append` from the empty upper is atomic, so exactly one
  snapshot attempt wins and `S` becomes a durable shared fact at commit
  time. Filtering against it is then deterministic (see Constraints).
- Requires a handoff protocol: recording `S`, verifying the main slot has
  not compacted past `S` before commit, and communicating the per-export
  start point to the dataflow.
- Requires a remap binding covering the reclocked time of `S`, so the main
  dataflow must keep minting bindings while the job runs.
- The dataflow still restarts once to pick up the export, but without
  stalling and without the resume LSN regression.

### C. Shadow pipeline with catch-up and handoff

Run a second, temporary pipeline for the new table only, with its own
temporary slot: snapshot, then follow CDC until it reaches an agreed LSN
`H`, commit through `H`, and hand off to the main pipeline for events after
`H`.

Properties:

- The main pipeline is unaffected until a restart at handoff time.
- Requires a second replication slot for the duration, with the associated
  WAL retention and connection cost on the upstream database.
- Requires a handoff protocol that guarantees exactly-once around `H`.

### D. Early rewind request emission only

Emit rewind requests when the snapshot transaction opens instead of after
`COPY`, so the replication operator starts streaming immediately. This
removes hold 1 but not hold 2: the shared capability remains pinned until
the snapshot completes, so other subsources still make no visible progress.
It bounds upstream slot lag during the snapshot but does not address
freshness.

### Sink staging to blob storage

This addresses the accumulation cost of option A. It is not a standalone
solution to the freshness problem.

**Where memory grows today.** The storage persist sink
(`src/storage/src/render/persist_sink.rs`) keeps one `BatchBuilder` per
distinct timestamp in its `write_batches` operator. `BatchBuilder::add`
(`src/persist-client/src/batch.rs`) uploads a part to blob storage whenever
its in-memory columnar buffer reaches `blob_target_size`, so large builders
already stage data to S3 incrementally. During hydration under option A,
reclocked CDC arrives at a large number of distinct fine-grained timestamps.
Each timestamp gets its own builder holding a small buffer that never
reaches the flush threshold. Resident memory therefore grows with the number
of distinct timestamps, plus fixed per-builder overhead, rather than with
data volume.

**Relevant existing behavior.**

- `mint_batch_descriptions` mints one batch description per frontier
  advancement. A frontier that jumps from the minimum to `T` when the
  rewind resolves yields a single description covering `[minimum, T)`.
- Every builder is finished with the full description bounds, and multiple
  batches for the same description are appended together in one
  `compare_and_append_batch`. Many batches under one description is the
  existing pattern.
- `BatchBuilder` takes its lower bound at creation and its upper bound only
  at `finish`. `finish(T)` fails if the builder contains any update at or
  beyond `T`.
- Eagerly uploaded parts of a builder that is dropped before finishing are
  leaked on restart. This is current behavior, unchanged by the options
  below.
- All cluster replicas are swap-only. No local disk is available to
  dataflow operators, so spilling operator state to local disk is not an
  option. Swap does not substitute: cold per-timestamp builders can page
  out while idle, but finishing all builders when the description arrives
  pages the entire set back in at once.

**Coalesced builders (sink-only change).** During hydration, route updates
into a small number of long-lived builders for the pinned region instead of
one builder per timestamp. Updates are added at their exact per-update
timestamps, parts flush to S3 at `blob_target_size` increments, and the
builders are finished with `[minimum, T)` when the description arrives.
Resident memory is bounded by `blob_target_size` per builder instead of by
distinct-timestamp count. Because `T` is not known in advance and a builder
containing updates at or beyond `T` cannot be finished at `T`, a two-tier
stash is required: coalesced builders hold times older than a trailing
horizon, and per-timestamp builders hold recent times. `T` is determined
when the snapshot resolves and corresponds to near-current reclocked time,
so it falls in the recent tier. No persist API changes are required, and
the crash leak behavior is unchanged from today.

### Persist extensions

With some changes to persist, we can avoid leaking the data written to blob storage for Option A:

1. **Shard-attached staged batches.** Today a batch only becomes durable when it's linked into shard
   state via `compare_and_append_batch`. Before that, it's an unlinked `ProtoBatch`: parts arr in S3
   (uploaded at blob_target_size increments), but nothing references them. A a crash leaks the
   parts and loses the work. During a long snapshot the coalesced builders can hold hours of
   concurrent CDC events in this unlinked state.

   To solve the leak, MZ could track these staged batches in a per-shard registry of named staged
   batches. Shard state would record it: the sink periodically finishes a builder and registers the
   batch without linking it into the readable trace. That makes the parts GC-tracked (no leak).
   Linking into the trace happens later, atomically, when the export's frontier can finally advance.

   This doesn't allow us to recover on a dataflow restart without extra bookkeeping.  On a restart,
   the snapshot LSN changes, which will affect the rewinds.

2. **Two-frontier shards.** Today a shard has one upper, appends must be contiguous,
   and everything below the upper is immutable and readable. This change allows the shard to carry
   two frontiers: a write frontier (how far appends have gone) and a readable upper (the highest
   frontier that doesn't include a sparse batch). The hydrating export's shard would have a sparse
   batch where the snapshot belongs, while regular CDC appends normally beyond it. This allows
   updates to be durable immediately, resumable from the written frontier after a restart. When the
   snapshot completes it fills the hole, and the readable upper jumps to the written frontier.

   There is special handling needed in the event the snapshot has to restart. The snapshot cannot
   be reproduced at the same offset, which means that data written to the shard needs to be
   retracted. An astute observer may realize that the readable upper is still and T::mininmum,
   so they shared has never been read. It may be reasonable to append a truncation marker to the
   shard to indicate "updates before this point are logically absent as of the marker's write,
   physically reclaimed at compaction", allowing the shard to restart from T::minimum.


For Option B:
1. **Multi-shard atomic commit.** txn-wal provides atomic writes across
   shards. Option B's handoff record (the snapshot data and the durable
   record of `S`) can be committed in one atomic operation using the
   existing mechanism.
   No persist change can make the new export itself readable before its
   snapshot completes, because contents at a readable timestamp are immutable
   and the pre-snapshot contents would be wrong.

## Work required for option A, by connector

### Pipeline facts

The dataflow downstream of the raw connector operators is already built per
export:

- One statistics operator and one reclock operator instance per export
  (`src/storage/src/source/source_reader_pipeline.rs`, the loops at the
  reclock and statistics stages).
- One decode and envelope fragment per export
  (`src/storage/src/render/sources.rs`).
- One persist sink per export, each writing its own data shard
  (`src/storage/src/render.rs`).
- The reclock operator derives its output frontier from its own captured
  input stream combined with the shared remap upper
  (`src/timely-util/src/reclock.rs`), so per-export input frontiers
  translate to per-export reclocked frontiers without changes.
- Remap bindings are minted from the probe channel, independent of data
  capabilities (`source_reader_pipeline.rs`, `remap_operator`), so bindings
  advance during a snapshot regardless of data capability holds.

The frontier coupling is confined to the connectors: each raw reader emits
one multiplexed `(output_index, data)` stream on a single timely output
port, demuxed with `partition()`, which routes data but propagates the
single upstream frontier to every output (`postgres.rs`, `kafka.rs`, and
the MySQL and SQL Server equivalents). `SourceRender::render` already
returns per-export collections (`src/storage/src/source/types.rs`), so the
changes below are internal to each connector and require no trait or
pipeline changes.

### PostgreSQL

1. Replace the `partition()` demux with one output port per export on the
   snapshot and replication operators. Exports are known at render time and
   ports can be created in a loop. Each port gets its own capability set.
2. Snapshot operator: close the ports of already hydrated exports
   immediately. Emit rewind requests when the snapshot transaction opens
   (the snapshot LSN is known as soon as the temporary slot is created)
   instead of after `COPY`. Hold a hydrating export's port at the minimum
   until its `COPY` completes.
3. Replication operator: remove the loop that drains the rewind input
   before opening the replication stream. Make the capability hold
   per port: hydrated exports downgrade with the data upper freely, a
   hydrating export's port holds at the minimum until the data upper passes
   its snapshot LSN. The inline dual emission of rewind negations is
   unchanged. The `resume_lsn <= snapshot_lsn + 1` validation moves to the
   earlier rewind arrival point.

Consequences to account for:

- The hydrating export's persist sink accumulates its concurrent CDC for
  the duration of the snapshot. The sink staging work above is a
  prerequisite for large or high-churn tables.
- Restart during a snapshot can now observe states that today's
  serialization makes impossible by construction. Each restart interleaving
  needs re-verification.
- WAL retention is unchanged. Slot acknowledgment follows the minimum
  committed upper across exports, which the hydrating export holds at the
  minimum until it completes.
- A source with hundreds of tables produces that many output ports on the
  reader operator. The downstream pipeline already has one operator chain
  per export, but the port-per-export reader shape needs validation at that
  scale.

### Kafka (upsert envelope)

The Kafka path has no rewind machinery. Retractions are derived by the
per-export upsert operator from its own state
(`src/storage/src/render/sources.rs`), and a new export's operator starts
from an empty shard. The "snapshot" of a new export is a re-read of the
topic from the export's start offsets. The reader currently resumes every
partition from the minimum resume upper across all exports
(`src/storage/src/source/kafka.rs`), so adding an export makes the single
consumer re-read the topic from the start offsets and no export commits new
data until the re-read reaches the tip.

1. Per-export output ports, as for PostgreSQL. Message duplication per
   export moves from the multiplexed stream into per-port emission.
2. A second, backfill consumer reads from the hydrating exports' start
   offsets while the steady consumer continues from the hydrated exports'
   committed positions. Handoff is a per-partition offset `H`: the
   hydrating export takes offsets below `H` from the backfill consumer and
   offsets at or above `H` from the steady feed. Kafka offsets are dense
   per partition, so the split is exact and requires no deduplication and
   no negation.
3. Per-export capabilities: hydrated exports' ports track the steady
   consumer's progress, a hydrating export's port tracks the backfill
   consumer and holds below the handoff point until the backfill reaches
   it.
4. The upsert operator needs no structural change. A hydrating export
   builds a second full copy of upsert state on the replica while its
   backfill runs, which is a memory cost on swap-only replicas. Per-export
   rehydration completion reporting already exists.
5. Backfilled old offsets reclock through existing remap bindings, and
   reclocked times not beyond the as-of are advanced to the as-of, which is
   existing reclock behavior.
6. The topic must retain data back to the export's start offsets. This
   holds for any design, since the data has no other source.

For append-only envelopes (none, Debezium), the backfill emits the same
insert-only records the steady path would, so the consumer split above is
sufficient and no per-key state is involved.

### MySQL and SQL Server

Both use the PostgreSQL rewind pattern. MySQL reuses the `RewindRequest`
type keyed by a GTID snapshot upper
(`src/storage/src/source/mysql/replication.rs`), SQL Server implements the
same structure with a per-export `initial_lsn` and `snapshot_lsn` pair
(`src/storage/src/source/sql_server/replication.rs`). The same three
changes apply: per-export output ports, rewind requests emitted when the
snapshot transaction is established (the snapshot upper is available at
that point in both systems, and SQL Server already tracks the per-export
pair), and per-port capability holds until the replication stream passes
each hydrating export's snapshot upper. The PostgreSQL consequences apply
equally: sink staging as a prerequisite, restart interleavings to
re-verify, and upstream log retention pinned by the minimum committed
upper for the duration of a snapshot.

In general, a connector supports independent export frontiers when it can
(a) emit each export on its own output port with its own capabilities,
(b) determine the snapshot consistency bound when the snapshot starts
rather than when it completes, (c) reconcile snapshot and stream either by
compensating negated emission at the minimum time (the rewind pattern) or
through an envelope that derives retractions from state (upsert), and
(d) derive per-export progress from its own read process.

## DBLog and derived approaches

### The DBLog algorithm

DBLog ([Andreakis and Papapanagiotou, Netflix, arXiv:2010.12597][dblog])
captures full table state without a consistent snapshot by interleaving
chunked selects with the live log stream:

1. Pause log event processing.
2. Write a low watermark: update a UUID in a dedicated single-row watermark
   table in the source database. The write appears in the transaction log.
3. Select the next chunk: rows in ascending primary key order, above the
   previous chunk's maximum key, at read-committed isolation.
4. Write a high watermark. Resume log processing.
5. Between the low and high watermark events in the log, drop any buffered
   chunk row whose primary key appears in a log event (the log wins, because
   the select's exact log position is unknown but is bracketed by the
   window). At the high watermark, emit the surviving chunk rows, then
   continue with the log.

Log processing stalls only during steps 2 through 4. Chunk progress is
checkpointed, so capture can pause and resume at chunk granularity and can
be triggered at any time for all tables, one table, or specific primary
keys. No locks and no long-running transaction are used.

The output contract is correspondingly weak. Chunk rows are full-row states
with no before-image. The stream preserves per-key history order but at no
point during capture corresponds to a consistent database state. Consumers
must apply per-key upsert semantics (the paper's canonical output is
log-compacted Kafka).

### Debezium's adaptations

Debezium implemented the algorithm as incremental snapshots
([DDD-3][ddd3], [blog][dbz-blog]), with a writable signaling table providing
the watermark events. Two read-only variants replace log-embedded watermarks
with query-only boundary captures, classifying each streamed event by
transaction ID instead of watching for watermark events:

- MySQL: compare each binlog event's GTID against `executed_gtid_set`
  captured before and after the chunk select
  ([blog][dbz-ro-blog]).
- PostgreSQL: compare each WAL event's transaction ID against
  `pg_current_snapshot()` captured before and after the chunk
  ([DDD-8][ddd8]). This requires PostgreSQL 13, requires a preceding
  `pg_current_xact_id()` call to force transaction ID assignment (which
  fails during recovery, so it cannot run on a hot standby), and cannot
  recognize duplicates written through subtransactions.

Debezium's incremental snapshot delivery model is explicitly weaker than the
paper's: consumers may receive an update before the corresponding snapshot
read of the same key, duplicate row states, and deletes for keys never
delivered ([DDD-3][ddd3]). Correct consumption requires per-key idempotent
upsert handling.

### Why this family does not fit Materialize

1. **Upsert output vs. differential collections.** Chunk rows carry no
   before-image, and log events for keys whose chunk has not yet been
   selected cannot be turned into correct retraction/insertion pairs
   without per-key prior state. Streaming DBLog output into a persist shard
   as diffs produces incorrect multiplicities. Converting it requires
   maintaining upsert state (key to current value) for the entire table for
   the duration of the capture.
2. **No consistent state during capture.** Materialize exposes readable
   collection contents at every timestamp between the since and the upper,
   and those contents must be a consistent state of the upstream table.
   Mid-capture DBLog output corresponds to no upstream state at any LSN.
3. **Nondeterministic interleaving violates definiteness.** Where chunks
   land relative to log events depends on process scheduling, so the
   interleaved stream differs across replicas and restarts. Readable
   contents derived from it are not a deterministic function of durable
   shared facts.
4. **Weakened ordering in the practical implementations.** Debezium's
   delivery model (duplicates, per-key reordering, deletes for undelivered
   keys) is designed for idempotent upsert consumers and is incompatible
   with exact-multiplicity ingestion.
5. **Primary key requirement.** Chunking and window deduplication are keyed
   on primary keys. Materialize's PostgreSQL source supports tables without
   primary keys (via `REPLICA IDENTITY FULL`), which have no chunkable key.
6. **Upstream write access.** The paper's watermark table requires write
   access to the source database, which the Materialize source contract does
   not include. The read-only variants remove table writes but the
   PostgreSQL variant still requires transaction ID assignment on a
   primary, requires PostgreSQL 13, and has a subtransaction deduplication
   gap. The MySQL variant requires GTID mode.

## References

- [DBLog: A Watermark Based Change-Data-Capture Framework (arXiv:2010.12597)][dblog]
- [Debezium DDD-3: Incremental snapshotting][ddd3]
- [Debezium DDD-8: Read-only incremental snapshots for PostgreSQL][ddd8]
- [Debezium blog: Incremental Snapshots in Debezium][dbz-blog]
- [Debezium blog: Read-only Incremental Snapshots for MySQL][dbz-ro-blog]
- Code: `src/storage/src/source/postgres/snapshot.rs`,
  `src/storage/src/source/postgres/replication.rs`,
  `src/storage/src/source/mysql/replication.rs`,
  `src/storage/src/source/sql_server/replication.rs`,
  `src/storage/src/source/kafka.rs`,
  `src/storage/src/source/source_reader_pipeline.rs`,
  `src/storage/src/render/sources.rs`,
  `src/storage/src/render/persist_sink.rs`,
  `src/timely-util/src/reclock.rs`,
  `src/persist-client/src/batch.rs`

[dblog]: https://arxiv.org/abs/2010.12597
[ddd3]: https://github.com/debezium/debezium-design-documents/blob/main/DDD-3.md
[ddd8]: https://github.com/debezium/debezium-design-documents/blob/main/DDD-8.md
[dbz-blog]: https://debezium.io/blog/2021/10/07/incremental-snapshots/
[dbz-ro-blog]: https://debezium.io/blog/2022/04/07/read-only-incremental-snapshots/
