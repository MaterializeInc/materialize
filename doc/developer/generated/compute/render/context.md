---
source: src/compute/src/render/context.rs
revision: 0b82784bb8
---

# mz-compute::render::context

Defines `Context`, the central data structure that accumulates dataflow-local collections and arrangements while a `RenderPlan` is being translated into Timely operators.
Provides `lookup_id` for resolving `Id` references to either locally rendered collections or imported arrangements, and `insert_id`/`update_id`/`remove_id` for managing `CollectionBundle` bindings keyed by `Id`.
Handles timestamp refinement: the context is parameterized by a scope `S` whose timestamp implements `RenderTimestamp`, which refines `mz_repr::Timestamp`; the two may differ in the case of regions or iteration, and the context provides `enter_region` to bring all bindings into a child scope.
`CollectionBundle` exposes two low-level flat-map helpers: `flat_map_core_fallible` builds a two-output operator returning `(ok_stream, err_stream)`, with `PendingWork` retaining one capability per output so work can be spread across activations; `flat_map_core_ok` is a single-output sibling for callers that never emit errors, backed by `PendingWorkOk` which holds a single capability. `as_specific_collection` uses `flat_map_core_ok`. The ok output container builder is generic at each call site.
