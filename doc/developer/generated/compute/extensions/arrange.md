---
source: src/compute/src/extensions/arrange.rs
revision: b0fa98e931
---

# mz-compute::extensions::arrange

Provides `MzArrange` and `MzArrangeCore` extension traits that wrap differential dataflow's `arrange_core` and automatically attach an `ArrangementSize` logging operator to every arrangement.
The `ArrangementSize` trait and its implementations compute heap size, capacity, and allocation counts per batch and emit `ComputeEvent::ArrangementHeapSize*` log events for introspection.
`KeyCollection` is a helper newtype for key-only (unit-value) collections, allowing them to flow through the same arrangement pipeline.
