---
source: src/storage/src/storage_state/async_storage_worker.rs
revision: a55caae279
---

# mz-storage::storage_state::async_storage_worker

Implements `AsyncStorageWorker`, a companion async task that the synchronous timely main loop delegates blocking async operations to.
Accepts `AsyncStorageWorkerCommand` variants (update ingestion frontiers, update sink frontiers, forward drop) via an unbounded channel, performs persist shard reads to compute resumption frontiers, and returns `AsyncStorageWorkerResponse` variants back via a crossbeam channel.
