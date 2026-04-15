---
source: src/adapter/src/coord/sequencer/inner/create_materialized_view.rs
revision: fcc110b5fe
---

# adapter::coord::sequencer::inner::create_materialized_view

Implements `sequence_create_materialized_view`, which runs the full materialized-view optimizer pipeline (MIR then LIR), persists the catalog entry with the resulting optimized expression, and installs the dataflow on the target cluster.
Handles `AS OF` clause and refresh schedule validation; also provides `sequence_alter_materialized_view_apply_replacement` for zero-downtime MV replacement.
