---
source: src/storage/src/upsert_continual_feedback_v2.rs
revision: 097450b020
---

# mz-storage::upsert_continual_feedback_v2

Implements an alternative feedback UPSERT operator that converts a stream of upsert commands `(key, Option<value>)` into a differential collection of `(key, value)` pairs, using a feedback loop through persist to maintain the "previous value" state needed for computing retractions.

The operator runs a loop where each iteration: ingests source upsert commands into a `MergeBatcher` (which consolidates entries for the same `(key, time)` by retaining the update with the highest `FromTime`/Kafka offset), checks the persist frontier probe to determine which times have been committed, and then seals and drains the batcher.
The seal step is skipped unless eligible entries are possible — specifically, unless `cap.time() <= persist_upper` and `persist_upper < input_upper` — to avoid an O(N) merge cost when nothing can be processed (e.g., during upstream snapshots or when the source races ahead of persist during rehydration).
Drained entries are classified by `drain_sealed_input` into three categories relative to `persist_upper`: entries at the frontier (`ts == persist_upper`) are eligible and processed against the current persisted state (retraction emitted for any existing value, new value inserted); entries above the frontier (`ts > persist_upper`) are ineligible and pushed back into the batcher for the next iteration; entries below the frontier (`ts < persist_upper`) have already been persisted by some writer and are dropped. Dropping below-upper entries mirrors v1's behavior and prevents the output capability from being pinned below the shard upper forever.
Output capability is downgraded to the minimum time of any buffered data and dropped when the batcher is empty.

`UpsertDiff` is a custom `Semigroup` diff type that carries the upsert payload and a `from_time` for deduplication; `UpsertKey` and `UpsertValue` are the key and value types exchanged over the dataflow.
The persist-feedback arrangement is backed by a `ValRowSpine<UpsertKey, _, _>` (using `ValRowColPagedBuilder`): keys are stored in a columnation arena (`UpsertKey` is a fixed-size `[u8; 32]` and uses `CopyRegion`) and values are stored as packed `Row` bytes in a `DatumContainer`, allowing the OS to evict cold value pages efficiently under memory pressure.
