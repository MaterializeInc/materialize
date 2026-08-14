# Cluster Branches: Forking a table shard in persist

- Parent: [`20260814_cluster_branches.md`](./20260814_cluster_branches.md)

This document details the persist primitive that branches need: a fork of a **table** shard at the branch point. Nothing else forks: the cluster's own indexes and materialized views re-render on the branch, and inputs from outside the cluster (sources, objects on other clusters) are read from their live shards. Only a writable table is forked. A read-only table, like a source, is read from its live shard.

## The fork

A branch that writes a table needs the table's data isolated: it starts from production's contents as of the branch point, then diverges, with production never seeing the branch's writes and the branch never seeing production's later writes. Two properties are required. Create is **metadata-only** (sub-second, no data copied), and the fork **does not tax production** (compaction should not be held back, no read amplification).

A fork is a new persist shard whose trace is seeded from the source shard's state at `branch_ts`:

- Inherited batches (at or below `branch_ts`) are referenced from the fork's trace, but their parts still live in the **source** shard's blobs. Nothing is copied.
- The branch's own writes append above `branch_ts` as ordinary, self-owned batches in the fork's own blobs.
- After initialization the fork is an ordinary shard, with two twists covered below: some parts are foreign, and one inherited batch is cut off.

Fork initialization mirrors `maybe_init_shard`, seeding the fork's first state from the source's snapshot at `branch_ts` (`since = min`, `upper = branch_ts + 1`) instead of an empty trace.

## Addressing the source's blobs

Blob keys are resolved relative to the shard: the fetch path prepends the shard id, so every part is assumed to live under its own shard's prefix. A fork's inherited parts do not.

Each batch part carries a `source_shard: Option<ShardId>` (on `HollowBatchPart`). The resolution rule is: use `source_shard` when set, the local shard when `None` to address keys.

## Cutting off post-branch data

Almost every inherited batch is entirely at or below `branch_ts` (`upper <= branch_ts + 1`). At most one batch per run straddles the point (`lower <= branch_ts < upper`): its blob holds updates both before and after `branch_ts`, and the fork must keep only the former. `ts_rewrite` advances timestamps and is not a cutoff, so this is new.

The straddling inherited batch is tagged with `cutoff_ts = branch_ts`, and its `upper` is clamped to `branch_ts + 1` in the fork's trace. The fetch path drops any update with `time > cutoff_ts` when reading that batch. Fork initialization only marks the batch, so create stays metadata-only.

Fork initialization takes a brief read hold on the source at `branch_ts`, pinning its `since` there while it references the inherited blobs, then releases it. The hold spans only creation: once the retained reference is registered, the immutable blobs keep their pre-`branch_ts` times regardless of later compaction. The hold succeeds whenever `since <= branch_ts`, which is guaranteed by how `branch_ts` is chosen. It is selected in the valid read interval over all inputs, so `since <= branch_ts` by construction (see the parent doc's Timestamps section), and the branch takes its read holds atomically at selection, so no input's `since` can advance past `branch_ts` before the hold is taken.

This self-corrects. When the fork later compacts the straddling batch together with its own batches, the filtered result is written into a fork-owned blob and the cutoff bakes in, so the read-time filter disappears through ordinary compaction with no work paid at create.

## Compaction and blob sharing

Production and the fork are separate traces, each with its own `since`. They are independent except that the fork's inherited parts point at production's blobs.

The model is that **branch points are compaction boundaries** on a shared history resulting in multiple `since` frontiers. Compaction consolidates within the segments the frontiers delimit and preserves each boundary, and a reader at any `since` reads the segments at or below it. Mapped onto the fork:

- **Above `branch_ts`:** production compacts its own trace freely, since the fork does not reference production's data up there and holds its own writes instead. The fork compacts its own writes freely. There is no interaction.
- **At or below `branch_ts`:** as production's `since` advances, production compacts its history as usual, writing new consolidated blobs. The only constraint is physical deletion: production's garbage collection must not delete a specific blob while the fork still points at it. Production is not held back from compacting, only from deleting the pinned blobs.
- **On teardown:** the fork's `since` and its references go away, so production's next collection reclaims those blobs and everything catches up.

## Retention through garbage collection

Garbage collection is driven by `seqno_since`: it removes blobs referenced only by state versions below the watermark, then truncates state. Retention is therefore already expressed as "a blob is kept while live state references it," and that is the path a fork's pin rides. Registration may change, but the deletion logic does not: it keeps referenced blobs and reclaims the moment the reference is gone, for any release reason.

The pin must retain blobs **without holding the source's compaction `since`**. The mechanisms are distinct: compaction advances `since` by writing new state versions, while blob deletion keys off `seqno_since`. No existing reader type separates them, though. The seqno capability that gates blob deletion lives only on `LeasedReaderState`, which also holds a `since`, and `CriticalReaderState` holds a `since` but no seqno. So the fork needs a **new retain-only reference kind** that the `seqno_since` collection honors without contributing a `since`.

The reference pins the specific inherited blob keys. At create, the fork's inherited parts are enumerated (`RunPart::part_stream`) and their keys registered as held on the source, so collection keeps exactly them. This keeps retention proportional to the branch point rather than to production's churn since the branch point.

This overhead on the source grows with the number of branches, not with how much production writes after the branch point. Each branch pins the source blobs that hold its inherited data. The pin lands in two places: the pinned blob keys go into the source's own state, and the original blobs stay alive even after production has recompacted that history into fresh ones, so the source keeps both copies. Neither part grows over time. Both are fixed when the branch is created and sized to the branch point, so ten branches cost about ten times as much, and each one's share is reclaimed when its branch is torn down.

## Making the fork a writable table

A branched table must accept `INSERT`, `UPDATE`, and `ALTER`. Table writes go through the txns (txn-wal) system, which sequences writes across table shards and advances their frontiers, including empty progress when there are no writes.

The fork shard is registered through the ordinary table-registration path: bind the branch table's `GlobalId -> fork ShardId` via `insert_collection_metadata` (populated through `ids_to_register`), and register it with `register_table_collections`. Branch writes then take the existing table-write path.

The fork shard is **not empty at registration**: it already carries the inherited batches, and its `upper` is `branch_ts + 1`. Txns registration normally advances a fresh data shard's upper with an empty append at the register timestamp (`register`). Registering the fork at a timestamp at or above `branch_ts + 1` treats its inherited content as pre-registration history, read via snapshot, with txns tracking it from the register timestamp forward.

## Lifecycle

Teardown (`DROP BRANCH`, expiry, or eviction, covered in the parent doc) releases the fork's retained reference on the source. Production's next collection then reclaims any blob no live fork still pins. The fork shard's own blobs, holding the branch's writes, are deleted with the fork. Release is one path for every teardown reason.

## Alternatives

### Blob addressing

- **Per-part `source_shard` (chosen).** Confines foreignness to fork batches.
- **Absolute, self-describing blob keys.** Cleaner abstractly, but changes the key model globally on the hot read path to serve a fork-only feature.

### Cutting off post-branch data

- **Read-time filter (chosen).** Marks the straddling batch and filters on read, keeping create metadata-only, and self-heals through compaction.
- **Init-time rewrite.** Clean reads forever, but does real IO at create for a cost compaction removes anyway.

### Compaction model

- **Fork shares blobs, per-trace sinces (chosen).** Writes above `branch_ts` need isolation regardless, so separate traces plus blob sharing gets the boundary semantics with least machinery.
- **Native multi-`since` on a single shard.** More general (time travel, many readers at different frontiers), but a large persist change.

### Retention

- **Use the existing collection path (chosen).** The pin is a retained reference on a normal state transition, and collection is unchanged in spirit.
- **A dedicated blob-reference table or per-blob refcounts.** Both require additional metadata management, and refcounts fight persist's immutable-blob model.

### Retention granularity

- **Pin the specific inherited keys (chosen).** Retention stays proportional to the branch point.
- **Hold a seqno at `branch_ts`.** Reuses existing machinery, no new reference kind, but `seqno_since` is a single watermark so it over-retains all of production's since-compacted history for the branch's life.
