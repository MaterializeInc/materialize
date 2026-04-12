---
source: src/compute/src/render/context.rs
revision: 84bda5baa0
---

# mz-compute::render::context

Defines `Context`, the central data structure that accumulates dataflow-local collections and arrangements while a `RenderPlan` is being translated into Timely operators.
Provides `lookup_id` for resolving `Id` references to either locally rendered collections or imported arrangements, and methods for inserting `AvailableCollections` (both unarranged and arranged) keyed by `GlobalId`.
Handles timestamp refinement: the context is parameterized by a scope `S` whose timestamp implements `RenderTimestamp`, which refines `mz_repr::Timestamp`; the two may differ in the case of regions or iteration, and the context provides methods to enter imported traces into the rendering scope.
