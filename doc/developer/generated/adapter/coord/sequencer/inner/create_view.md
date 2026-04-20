---
source: src/adapter/src/coord/sequencer/inner/create_view.rs
revision: a632912d24
---

# adapter::coord::sequencer::inner::create_view

Implements `sequence_create_view` and `sequence_create_views`, which run the view optimizer pipeline and persist the resulting `OptimizedMirRelationExpr` to the catalog.
Views are stored as optimized MIR expressions and inlined into downstream dataflows at query time.
