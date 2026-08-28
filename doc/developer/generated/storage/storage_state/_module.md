---
source: src/storage/src/storage_state.rs
revision: 844b947128
---

# mz-storage::storage_state

Defines `StorageState` and `Worker`, the per-timely-worker state and main loop for the storage cluster.
`StorageState` holds all live source/sink tokens, shared frontiers, statistics aggregators, persist clients, txns context, and configuration.
`Worker` drives the main event loop: it processes external `StorageCommand`s from the controller (delegating async frontier lookups to `AsyncStorageWorker`), handles `AsyncStorageWorkerResponse`s, and processes sequenced `InternalStorageCommand`s that actually render or drop dataflows.
This design ensures that dataflow-rendering commands reach all workers in a consistent total order via the internal command sequencer.
External commands never render dataflows directly; they broadcast internal commands so that all timely workers process them in the same order.
The maintenance loop applies `ENABLE_UPSERT_PAGED_SPILL` to both upsert-v2 stash flavors' spill mechanisms: `upsert_stash_spill::set_enabled` for the chunked flavor's process-wide chunk spill gate, and `upsert_stash_pager::set_enabled` for the paged flavor's storage-owned column pager. Both the buffer pool and pager budget are shared with compute; storage only sets its own participation leg.

## Submodules

- `async_storage_worker` — companion async worker for operations that require an async runtime, since the timely main loop is synchronous.
