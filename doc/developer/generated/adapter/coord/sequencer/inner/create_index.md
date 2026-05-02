---
source: src/adapter/src/coord/sequencer/inner/create_index.rs
revision: a632912d24
---

# adapter::coord::sequencer::inner::create_index

Implements `sequence_create_index`, which runs the index optimizer pipeline, persists the catalog entry, and dispatches the resulting dataflow to the appropriate compute cluster.
Also handles `sequence_create_index_finish` for completing the index creation after any async pre-flight steps.
