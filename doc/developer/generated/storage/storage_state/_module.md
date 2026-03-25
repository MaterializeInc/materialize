---
source: src/storage/src/storage_state.rs
revision: 6904a6baf2
---

# mz-storage::storage_state

Defines `StorageState` and `Worker`, the per-timely-worker state and main loop for the storage cluster.
`StorageState` holds all live source/sink tokens, shared frontiers, statistics aggregators, persist clients, txns context, and configuration.
`Worker` drives the main event loop: it processes external `StorageCommand`s from the controller (delegating async frontier lookups to `AsyncStorageWorker`), handles `AsyncStorageWorkerResponse`s, and processes sequenced `InternalStorageCommand`s that actually render or drop dataflows.
This design ensures that dataflow-rendering commands reach all workers in a consistent total order via the internal command sequencer.
External commands never render dataflows directly; they broadcast internal commands so that all timely workers process them in the same order.

## Submodules

- `async_storage_worker` — companion async worker for operations that require an async runtime, since the timely main loop is synchronous.
