---
source: src/row-spine/src/arc_batch.rs
revision: 98ea0cc1cc
---

# mz-row-spine::arc_batch

Provides `ArcBatch<B>` and `ArcBuilder<Bld>`, a local newtype pair that carries differential dataflow's batch traits on an `Arc`-backed handle rather than the default `Rc`.

Differential's `Rc`-backed spines are worker-local: an `Rc<B>` cannot be sent across threads, so batches cannot be read by any thread other than the worker that maintains the trace. `ArcBatch<B>` wraps `Arc<B>` and re-implements all of differential's batch traits (`BatchReader`, `Batch`, `Navigable`, `Cursor`) by delegating straight through to the inner `B`. When `B`'s contents are `Send + Sync`, the resulting `ArcBatch<B>` is also `Send + Sync`, enabling cross-thread arrangement sharing. The orphan rule forbids a blanket `impl Trait for Arc<B>` here because both `Arc` and the relevant traits are foreign, so the newtype is required.

`Clone` on `ArcBatch<B>` clones the `Arc` handle, not the batch contents, matching the semantics of the `Rc`-backed default.

## Types

* `ArcBatch<B>` — transparent newtype around `Arc<B>`. Implements `BatchReader`, `Batch`, and `Navigable` by delegation; `Deref`s to `B` so callers can reach batch fields directly (e.g., `batch.0` reaches the inner `Arc` for pointer-identity tracking in batch-size logging).
* `ArcBatchCursor<C>` — cursor over an `ArcBatch`, delegating to the inner batch's cursor type `C`. The `Storage` associated type is `ArcBatch<C::Storage>`.
* `ArcBuilder<Bld>` — `Builder` wrapping an inner builder `Bld`; seals each completed batch into an `Arc` via `ArcBatch::new`. Used as the builder type for all `ArcBatch`-backed spines.
* `ArcMerger<B>` — `Merger` for `ArcBatch<B>`, delegating merge work to `B::Merger` and wrapping the result in `ArcBatch::new`.
