---
source: src/compute/src/render/context.rs
revision: 52f2de096d
---

# mz-compute::render::context

Defines `Context`, the central data structure that accumulates dataflow-local collections and arrangements while a `RenderPlan` is being translated into Timely operators.
Provides `lookup_id` for resolving `Id` references to either locally rendered collections or imported arrangements, and `insert_id`/`update_id`/`remove_id` for managing `CollectionBundle` bindings keyed by `Id`.
Handles timestamp refinement: the context is parameterized by a scope `S` whose timestamp implements `RenderTimestamp`, which refines `mz_repr::Timestamp`; the two may differ in the case of regions or iteration, and the context provides `enter_region` to bring all bindings into a child scope.
