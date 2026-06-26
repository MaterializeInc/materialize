---
source: src/adapter/src/coord/sequencer/inner/create_view.rs
revision: 277b33e9c0
---

# adapter::coord::sequencer::inner::create_view

Implements `sequence_create_view` and `sequence_create_views`, which run the view optimizer pipeline and persist the resulting `OptimizedMirRelationExpr` to the catalog.
Views are stored as optimized MIR expressions and inlined into downstream dataflows at query time.
`create_view_validate` constructs a `PlanValidity` that tracks the resolved dependency IDs and role metadata so that concurrent drops are caught between stages instead of panicking during catalog application.
