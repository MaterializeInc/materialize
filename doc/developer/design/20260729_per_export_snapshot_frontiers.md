# Per-Export Frontiers for Source Snapshots

- Associated:
  [Subsource Snapshot Freshness: Options Summary](20260721_subsource_snapshot_freshness_research.md)

## The Problem

Adding a table to a running source (`CREATE TABLE ... FROM SOURCE`, or the
legacy `ALTER SOURCE ... ADD SUBSOURCE`) restarts the ingestion dataflow. On
the new incarnation, the added table has an empty resume upper and is
snapshotted. Until that snapshot completes, no subsource of the source makes
visible progress. CDC events for already hydrated tables are not processed,
and their persist uppers do not advance. A snapshot of one large table can
stall every other subsource for hours.

If multiple tables are added together, snapshotting may complete for the
smaller tables, but they cannot make progress until the largest table is
also done. Any error during this time restarts snapshotting for all tables.

For the rewind-pattern connectors (PostgreSQL, MySQL, SQL Server) the stall
has two causes, both in the upstream reader operators:

1. The replication operator drains its rewind input to completion before processing any CDC events.
   Rewind requests are only sent after `COPY` (for OLTP) completes, so the CDC stream is not read
   while a snapshot runs.
2. All exports share a single data capability. While any rewind request is pending, the capability
   cannot be downgraded past the minimum offset, because the operator must still be able to emit
   negated data at the minimum. One hydrating export pins the frontier of every export.

Kafka stalls for a different reason. Multiple tables can be created from one
Kafka source, for example to accommodate schema changes in the topic. The
reader resumes every partition from the minimum resume upper across all
exports, so adding a table makes the single consumer re-read the topic from
the new table's start offsets, and the shared output port means no export
commits new data until the re-read reaches the tip.

Everything downstream of the reader operators is already per export: statistics operator, reclock,
decode and envelope, and persist sink.

## Success Criteria

- While a newly added table snapshots, the already hydrated exports of the
  same source continue to process CDC events and their persist uppers
  continue to advance.
- When multiple tables are added together, each commits its snapshot and
  begins advancing as soon as its own snapshot completes.
- Definiteness is preserved. The contents of every collection at each
  readable timestamp remain a deterministic function of durable shared
  facts. The output must not depend on any value known only to a single replica or dataflow
  incarnation, such as the snapshot LSN of the temporary slot.
- Exact multiplicities are preserved. No design element requires primary
  keys or upsert semantics from a connector that does not already have
  them.
- The upstream privilege contract is unchanged. For example, the PostgreSQL source
  continues to require only `SELECT` on the replicated tables and the
  `REPLICATION` attribute.
- Memory on the replica during hydration is bounded by a configurable
  target independent of snapshot duration and of the number of distinct
  reclocked timestamps. Replicas are swap-only, so unbounded operator state
  is not acceptable.
- A restart during a snapshot remains correct. The snapshot restarts from
  the beginning, as today.

## Out of Scope

The following are follow-on work. The design does not depend on them, and
they can land independently later.

- **Recovering accumulated work across restarts.** A restart during
  hydration discards the hydrating export's snapshot progress and its
  accumulated concurrent CDC, both are redone. Crash-safe accumulation
  needs persist extensions (a staged batch registry, or placeholder batches
  filled after the fact with a truncate marker for discarded attempts) plus
  a connector-side watermark protocol. The research document describes
  these. Nothing in this design forecloses them.
- **Avoiding the dataflow restart.** The export set is fixed when the
  dataflow is rendered, so adding a table still tears down and re-renders
  the ingestion dataflow. This design removes the stall that follows the
  restart, not the restart itself.
- **The resume LSN regression.** The replication resume point remains the
  minimum across all exports, so the stream still replays the gap between
  the slot's consistent point and the existing exports' committed uppers
  after a restart. Existing exports now make progress during that replay,
  but the replay itself remains.
- **Upstream log retention.** Slot acknowledgment continues to follow the
  minimum committed upper across exports, so WAL is retained upstream for
  the duration of a snapshot, as today.
- **Reading the new export before its snapshot completes.** Contents at a
  readable timestamp are immutable in persist, and pre-snapshot contents
  would be wrong. No design can offer this.

## Solution Proposal

Give each export its own output port and capability on the connector reader
operators, and hold back only the hydrating exports. For the rewind-pattern
connectors the negation-based rewind protocol is unchanged, so the existing
definiteness argument carries over without new durable state. For Kafka the
reconciliation already comes from the upsert operator's state, and the
change is a second consumer that backfills the hydrating exports.

The work is confined to the connectors. `SourceRender::render` already
returns per-export collections, reclock derives per-export output frontiers
from per-export input frontiers without changes, and remap bindings are
minted from the probe channel independent of data capabilities.

Steps 1 and 4 apply to all connectors. Steps 2 and 3 are the rewind-pattern
changes, the Kafka section below is their Kafka equivalent.

### 1. One output port per export

Replace the multiplexed `(output_index, data)` output and its `partition()`
demux with one output port per export on the snapshot and replication
operators. Exports are known at render time, ports are created in a loop,
and each port gets its own capability set.

This step is behavior preserving on its own: with all port capabilities
downgraded in lockstep, every export observes the same frontier as today.
It has landed for PostgreSQL and is the stepping stone for the steps below.

Because `give_fueled`'s yield accounting is per output handle, operators
that emit across many ports enforce the aggregate outstanding-byte bound
with a shared counter against `MAX_OUTSTANDING_BYTES`.

### 2. Early snapshot bound

The snapshot operator emits `RewindRequest { output_index, snapshot_lsn }`
when the snapshot transaction opens instead of after `COPY` completes. The
snapshot LSN is the temporary slot's consistent point and is known as soon
as the slot is created, before any data is copied. The
`resume_lsn <= snapshot_lsn + 1` validation moves to this earlier point.

The replication operator no longer drains the rewind input before opening
the replication stream. It starts streaming immediately, knowing from the
early rewind requests which exports need dual emission.

### 3. Per-export capability policy

Snapshot operator: ports of exports whose resume upper is past the minimum
are closed immediately. A hydrating export's port holds at the minimum
until its snapshot (e.g. `COPY`) completes, then closes.

Replication operator: ports of hydrated exports downgrade freely with the
data upper. A hydrating export's port holds at the minimum until the data
upper passes its snapshot LSN, because events at or below the snapshot LSN
must still be emitted negated at the minimum for that export. Once the
upper passes the snapshot LSN, the port downgrades with the data upper like
the rest. The inline dual emission of rewind negations is unchanged.

The slot is acknowledged at the minimum committed upper across exports, so
a hydrating export continues to pin WAL retention, but no longer pins any
other export's frontier.

### 4. Bounded sink accumulation

While a hydrating export's frontier is pinned, its concurrent CDC
accumulates in the storage persist sink. The sink keeps one `BatchBuilder`
per distinct timestamp, and `BatchBuilder` flushes parts to blob storage
only when a single builder reaches `blob_target_size`. Reclocked CDC
arrives at many distinct fine-grained timestamps, so resident memory grows
with the number of distinct timestamps rather than with data volume.

The sink change is a two-tier stash in `write_batches`. The tier is chosen
per update when the update is added:

- Each worker keeps one long-lived coalesced `BatchBuilder`, created with
  the minimum lower bound. `BatchBuilder::add` accepts each update at its
  exact timestamp and the upper bound is supplied only at `finish`, so one
  builder absorbs updates at any number of distinct timestamps and uploads
  a part to blob storage every time its buffer reaches `blob_target_size`.
  This capability exists today, the current code just never uses it across
  timestamps.
- The operator maintains a horizon `H`, defined as the maximum timestamp
  observed on its data input minus a configured lag `L`. `H` never
  retreats. An arriving update at time `t` is added to the coalesced
  builder when `t < H` and to today's per-timestamp builder for `t`
  otherwise.
- When the batch description `[minimum, T)` arrives after the rewind
  resolves, the coalesced builder is finished at `T`. `finish(T)` requires
  that the builder contain no update at or beyond `T`, which holds exactly
  when `H <= T`. Per-timestamp builders with times below `T` are finished
  with the description bounds as today. Builders at or beyond `T` are
  retained for the next description, which is also existing behavior.

`H <= T` is not structurally guaranteed. Timely gives no ordering between
data delivery and frontier propagation, so updates at or beyond `T` can
arrive before the description does and, with too small a lag, age into the
coalesced builder. Such an update is emitted by the reader only after it
downgrades its capability past `T`, so its timestamp exceeds `T` by at
most the reclocked time that elapses while the description is in flight.
`H` therefore exceeds `T` only if that propagation delay exceeds `L`, and
`L` is chosen orders of magnitude larger (minutes of reclocked time
against in-flight delays of at most seconds). The failure is detected when
the description arrives. The builder cannot be split, so the sink raises
an error and the dataflow restarts, discarding the unlinked batch.
Correctness is never at risk, only progress, and the same restart path
already exists for snapshot errors.

Resident memory is then bounded on both tiers. The coalesced builder holds
at most `blob_target_size` plus one part upload in flight. The
per-timestamp tier only ever spans the trailing window `L`, so its builder
count is bounded by `L` divided by the reclock interval rather than by
snapshot duration. With descriptions arriving regularly (the steady state)
builders are finished before their times age past `H`, the coalesced
builder stays empty, and behavior is unchanged. The coalesced tier fills
only when the frontier stalls for longer than `L`, which is the hydration
case. No persist API changes are required, and the crash leak behavior of
eagerly uploaded parts is unchanged from today.

### Kafka: backfill consumer with offset handoff

Kafka has no rewind machinery. Retractions are derived by the per-export
upsert operator from its own state, so the "snapshot" of a new export is a
re-read of the topic from the export's start offsets, and a new export's
upsert operator starts from an empty shard.

- A second, backfill consumer reads from the hydrating exports' start
  offsets while the steady consumer continues from the hydrated exports'
  committed positions. The handoff is a per-partition offset `H`: the
  hydrating export takes offsets below `H` from the backfill consumer and
  offsets at or above `H` from the steady feed. Kafka offsets are dense per
  partition, so the split is exact and needs no deduplication and no
  negation.
- The backfill consumer is a second rdkafka client. A client has one fetch
  position per topic-partition, and the backfill reads the same partitions
  at older offsets than the steady consumer, so one client cannot serve
  both positions concurrently. Clients do not share connections, so the
  backfill adds one connection per broker per worker for the duration of
  the hydration, matching the steady consumer's footprint, and is torn
  down at handoff. One backfill client per worker serves all hydrating
  exports, reading once from the minimum of their start offsets, so the
  cost does not grow with the number of exports added together. Because no
  other export waits on the backfill, it can be paced to limit broker
  load.
- Per-export capabilities: hydrated exports' ports track the steady
  consumer's progress. A hydrating export's port tracks the backfill
  consumer and holds below the handoff point until the backfill reaches it.
- The upsert operator needs no structural change. A hydrating export builds
  a second full copy of upsert state on the replica while its backfill
  runs, which is a memory cost on swap-only replicas.
- Backfilled old offsets reclock through existing remap bindings, and
  reclocked times not beyond the as-of are advanced to the as-of, which is
  existing reclock behavior.
- The topic must retain data back to the export's start offsets. This holds
  for any design, since the data has no other source.

For append-only envelopes (none, Debezium), the backfill emits the same
insert-only records the steady path would, so the consumer split is
sufficient and no per-key state is involved.

### Restart semantics

A restart during hydration discards the accumulated updates, they were
never linked into the shard, and the snapshot restarts from the beginning,
for the rewind connectors with a new temporary slot and a new snapshot LSN,
for Kafka with a new backfill from the export's start offsets. This matches
today's semantics. The serialization that today makes some restart
interleavings impossible by construction is removed, so
restart-during-snapshot paths need explicit re-verification (see Open
questions).

### Rollout

PostgreSQL first, then MySQL and SQL Server, which use the same rewind
structure adapted to their offset types. MySQL keys `RewindRequest` by a
GTID snapshot upper, SQL Server already tracks a per-export `initial_lsn`
and `snapshot_lsn` pair. In both, the snapshot upper is available when the
snapshot transaction is established, so the same three operator changes
apply. Kafka follows, since its backfill consumer is new machinery rather
than an adaptation of the rewind changes.

The capability policy change is gated by a feature flag, default off in
production and default on in CI so the new path is exercised by the test
suites before it is enabled.

In general, a connector supports independent export frontiers when it can
(a) emit each export on its own output port with its own capabilities,
(b) determine the snapshot consistency bound when the snapshot starts
rather than when it completes, (c) reconcile snapshot and stream either by
compensating negated emission at the minimum time or through an envelope
that derives retractions from state, and (d) derive per-export progress
from its own read process.

## Minimal Viable Prototype

Step 1 is implemented for PostgreSQL: the snapshot and replication
operators emit one output stream per export with capabilities managed in
lockstep, and the `partition()` demux is replaced by pairwise
concatenation. The pg-cdc test suite passes unchanged, validating the
operator shape and the aggregate emission accounting before any behavior
changes.

## Considered but Rejected

The research document records the full option space. Summary of the
rejected options and why:

- **Out-of-band snapshot committed directly to persist.** Run the snapshot
  as a one-shot job and `compare_and_append` the result from the empty
  upper at the reclocked snapshot LSN, then filter the stream against that
  durably recorded LSN. Definiteness-sound and free of sink accumulation,
  but it requires new infrastructure that option A does not: a one-shot job
  outside the ingestion dataflow, a durable handoff record, verification
  that the slot has not compacted past the recorded LSN, and per-export
  start points threaded into the dataflow. Option A reuses the existing
  rewind argument with connector-local changes only.
- **Shadow pipeline with catch-up and handoff.** A second temporary
  pipeline with its own replication slot snapshots the new table, follows
  CDC to an agreed LSN, and hands off. Rejected for the second slot's WAL
  retention and connection cost on the upstream database and for the
  exactly-once handoff protocol it requires.
- **Early rewind emission alone.** Emitting rewind requests at snapshot
  start without per-export capabilities removes the stream-draining gate
  but not the shared capability hold, so other exports still make no
  visible progress. It is subsumed by this design as step 2.
- **DBLog-style watermark snapshots.** Interleaving chunked snapshot reads
  with the stream, deduplicating by primary key between watermarks,
  produces a per-key upsert stream with no consistent point-in-time state.
  Rejected because storage collections require exact multiplicities, the
  nondeterministic interleaving violates definiteness, primary keys are not
  guaranteed, and the write-access variants violate the upstream privilege
  contract.
- **Persist extensions as the primary mechanism.** Gap appends or
  placeholder batches would make concurrent CDC durable immediately and
  remove the sink accumulation problem entirely, and a staged batch
  registry would make the coalesced builders crash-safe. Rejected for the
  initial delivery because they change persist shard state, compaction, and
  read paths, while the sink-only two-tier stash meets the memory bound
  with no persist changes. They remain candidate follow-ons for restart
  work recovery (see Out of Scope).

## Open Questions

- **Port count at scale.** A source with hundreds of tables produces that
  many output ports on the reader operators. The downstream pipeline
  already has one operator chain per export, but the port-per-export reader
  shape and its progress-tracking overhead need validation at that scale.
- **Restart interleavings.** Which restart-during-snapshot interleavings
  become newly possible once the snapshot and replication phases overlap,
  and what test coverage (platform checks, testdrive) demonstrates each is
  handled.
- **Two-tier horizon.** How the lag `L` is chosen and validated (a fixed
  configurable value, or adaptive to observed description propagation
  delay), and whether the restart-on-`H > T` fallback needs a metric and
  alert since it silently costs a rehydration.
- **Multiple concurrent hydrations.** Whether the sink change needs
  per-export tuning when several large tables hydrate at once on one
  replica.
- **Upsert state during backfill.** A hydrating Kafka export builds a full
  copy of its upsert state while its backfill runs. Whether that transient
  cost needs mitigation on swap-only replicas, for example by pacing the
  backfill consumer, and how it interacts with several exports backfilling
  at once.
