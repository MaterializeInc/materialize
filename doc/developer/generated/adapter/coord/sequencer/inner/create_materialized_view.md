---
source: src/adapter/src/coord/sequencer/inner/create_materialized_view.rs
revision: 7f632d2b4a
---

# adapter::coord::sequencer::inner::create_materialized_view

Implements `sequence_create_materialized_view`, which runs the full materialized-view optimizer pipeline (MIR then LIR), persists the catalog entry with the resulting optimized expression, and installs the dataflow on the target cluster.
Optimizer notices are rendered (via `CatalogState::render_notices_core` with an `ExprHumanizerExt` that resolves the new MV's own `global_id`) and the expression cache is populated before the catalog transaction, so that the durable cache is visible to other processes as soon as the item appears. Raw notices are emitted to the user session only after the catalog transaction succeeds.
Handles `AS OF` clause and refresh schedule validation; also provides `sequence_alter_materialized_view_apply_replacement` for zero-downtime MV replacement.
