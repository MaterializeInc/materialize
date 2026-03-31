---
source: src/compute/src/render/context.rs
revision: c642b63c77
---

# mz-compute::render::context

Defines `Context`, the central data structure that accumulates dataflow-local collections and arrangements while a `RenderPlan` is being translated into Timely operators.
Provides `lookup_id` for resolving `Id` references to either locally rendered collections or imported arrangements, and methods for inserting `AvailableCollections` (both unarranged and arranged) keyed by `GlobalId`.
Handles timestamp refinement: the context has both the outer scope timestamp `S::Timestamp` and an inner rendering timestamp `T`, and provides methods to enter imported traces from the outer scope into the inner rendering scope.
