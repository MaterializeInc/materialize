---
source: src/storage/src/upsert_continual_feedback_v2.rs
revision: a5208d1359
---

# mz-storage::upsert_continual_feedback_v2

Implements an alternative feedback UPSERT operator that converts a stream of upsert commands `(key, Option<value>)` into a differential collection of `(key, value)` pairs, using a feedback loop through persist to maintain the "previous value" state needed for computing retractions.

The operator runs a loop where each iteration: ingests source upsert commands into a `MergeBatcher` (which consolidates entries for the same `(key, time)` by retaining the update with the highest `FromTime`/Kafka offset), checks the persist frontier probe to determine which times have been committed, and then seals and drains the batcher.
The seal step is skipped unless eligible entries are possible — specifically, unless `cap.time() <= persist_upper` and `persist_upper < input_upper` — to avoid an O(N) merge cost when nothing can be processed (e.g., during upstream snapshots or when the source races ahead of persist during rehydration).
Drained entries classified as eligible (where the persist frontier is exactly at the update's time) are processed against the current persisted state — a retraction is emitted for any existing value and the new value is inserted; ineligible entries (between the persist and input frontiers) are pushed back into the batcher for the next iteration.
Output capability is downgraded to the minimum time of any buffered data and dropped when the batcher is empty.

`UpsertDiff` is a custom `Semigroup` diff type that carries the upsert payload and a `from_time` for deduplication; `UpsertKey` and `UpsertValue` are the key and value types exchanged over the dataflow.
