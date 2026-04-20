---
source: src/adapter/src/coord/indexes.rs
revision: a632912d24
---

# adapter::coord::indexes

Provides coordinator methods to query which indexes are available on a given collection in a given cluster and to build the set of `IndexImport`s needed for a dataflow.
Used by the optimizer and the peek sequencer to discover available arrangements during query planning.
