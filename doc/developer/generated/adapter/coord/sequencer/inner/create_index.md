---
source: src/adapter/src/coord/sequencer/inner/create_index.rs
revision: 6a75ad9bb5
---

# adapter::coord::sequencer::inner::create_index

Implements `sequence_create_index`, which runs the index optimizer pipeline, persists the catalog entry, and dispatches the resulting dataflow to the appropriate compute cluster.
Optimizer notices are rendered (via `CatalogState::render_notices_core` with an `ExprHumanizerExt` that resolves the new index's own `global_id`) and the expression cache is populated before the catalog transaction, so that the durable cache is visible to other processes as soon as the item appears. Raw notices are emitted to the user session only after the catalog transaction succeeds.
Also handles `sequence_create_index_finish` for completing the index creation after any async pre-flight steps.
`create_index_validate` constructs a `PlanValidity` with the resolved dependency IDs, target cluster, and role metadata so that concurrent drops are caught between stages instead of panicking during catalog application.
